import psycopg2
import networkx as nx
from networkx.algorithms import bipartite
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user":     os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode":  os.getenv("DB_SSLMODE", "disable"),
}

INSERT_QUERY = """
    INSERT INTO conflict_theaters_snapshots
    (snapshot_date, snapshot_ts, theater_id, name, nb_actors, actors, side_a, side_b)
VALUES
    (CURRENT_DATE, now(), %(theater_id)s, %(name)s, %(nb_actors)s, %(actors)s, %(side_a)s, %(side_b)s)
    ON CONFLICT (snapshot_date, theater_id)
DO UPDATE SET
    snapshot_ts = EXCLUDED.snapshot_ts,
    name        = EXCLUDED.name,
    nb_actors   = EXCLUDED.nb_actors,
    actors      = EXCLUDED.actors,
    side_a      = EXCLUDED.side_a,
    side_b      = EXCLUDED.side_b
WHERE conflict_theaters_snapshots.snapshot_ts < EXCLUDED.snapshot_ts;"""

def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )

def get_conflict_theaters(days=14, min_events=3):
    query = """
        SELECT
            CASE
                WHEN AGGRESSOR = 'TTP' THEN 'Afghanistan'
                ELSE AGGRESSOR
            END AS AGGRESSOR,
            CASE
                WHEN TARGET = 'TTP' THEN 'Afghanistan'
                ELSE TARGET
            END AS TARGET,
            COUNT(*) AS EVENT_COUNT
        FROM
            MILITARY_ACTIONS MA
            NATURAL JOIN TWEETS T
        WHERE
            CREATED_AT >= NOW() - INTERVAL '%s days'
            AND AGGRESSOR IS NOT NULL
            AND TARGET IS NOT NULL
            AND AGGRESSOR <> TARGET
            AND IS_DUPLICATE = 'false'
        GROUP BY
            AGGRESSOR,
            TARGET
        HAVING
            COUNT(*) >= %s
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (days, min_events))
            rows = cur.fetchall()

    # Compute total event volume per actor
    actor_volume = {}
    edges = []

    for aggressor, target, event_count in rows:
        edges.append((aggressor, target))
        actor_volume[aggressor] = actor_volume.get(aggressor, 0) + event_count
        actor_volume[target]    = actor_volume.get(target, 0)    + event_count

    G = nx.Graph()
    G.add_edges_from(edges)

    theaters = []
    for i, component in enumerate(
        sorted(nx.connected_components(G), key=len, reverse=True)
    ):
        top2 = sorted(component, key=lambda a: actor_volume.get(a, 0), reverse=True)[:2]
        name = f"{top2[0]} / {top2[1]} War" if len(top2) >= 2 else top2[0]
        sides = detect_sides(rows, component)
        if sides:
            side_a, side_b = sides
        else:
            side_a, side_b = [], []

        theaters.append({
            "theater_id": i + 1,
            "name":       name,
            "nb_actors":  len(component),
            "actors":     ", ".join(sorted(component)),
            "side_a":     ", ".join(side_a),
            "side_b":     ", ".join(side_b),
        })

    return pd.DataFrame(theaters)

def detect_sides(rows, component):
    DG = nx.DiGraph()
    actors_in_component = set(component)
    
    for aggressor, target, event_count in rows:
        if aggressor in actors_in_component and target in actors_in_component:
            if DG.has_edge(aggressor, target):
                DG[aggressor][target]['weight'] += event_count
            else:
                DG.add_edge(aggressor, target, weight=event_count)

    UG = DG.to_undirected()
    
    if bipartite.is_bipartite(UG):
        side_0, side_1 = bipartite.sets(UG)
        return sorted(side_0), sorted(side_1)
    else:
        return _greedy_two_color(UG)


def _greedy_two_color(G):
    """Coloration gloutonne en 2 couleurs (ignore les conflits de couleur)."""
    color = {}
    for node in G.nodes():
        if node in color:
            continue
        color[node] = 0
        queue = [node]
        while queue:
            n = queue.pop()
            for neighbor in G.neighbors(n):
                if neighbor not in color:
                    color[neighbor] = 1 - color[n]
                    queue.append(neighbor)
    
    side_0 = sorted(n for n, c in color.items() if c == 0)
    side_1 = sorted(n for n, c in color.items() if c == 1)
    return side_0, side_1


def save_snapshot(df: pd.DataFrame) -> int:
    """Insert or update today's snapshot. Returns the number of rows upserted."""
    records = df[["theater_id", "name", "nb_actors", "actors", "side_a", "side_b"]].to_dict(orient="records")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(INSERT_QUERY, records)
        conn.commit()
    return len(records)

if __name__ == "__main__":
    df = get_conflict_theaters()
    upserted = save_snapshot(df)