import os
import psycopg2
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import timedelta
from math import radians, sin, cos, sqrt, atan2
from collections import defaultdict

load_dotenv()

# ── Database connection ───────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user":     os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
}

# ── Thresholds ────────────────────────────────────────────────────────────────
SIM_THRESHOLD     = 0.82   # semantic similarity alone → duplicate
SIM_GEO_THRESHOLD = 0.72   # lower threshold when tweets are in the same area
GEO_RADIUS_KM     = 100    # radius in km to consider two tweets "same area"
TIME_WINDOW       = timedelta(hours=24)


# ── Helpers ───────────────────────────────────────────────────────────────────
def distance_km(c1, c2):
    """Distance in km between two (lat, lon) points."""
    R = 6371.0
    lat1, lon1 = radians(c1[0]), radians(c1[1])
    lat2, lon2 = radians(c2[0]), radians(c2[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))


def are_duplicates(i, j, sim_matrix, timestamps, typologies, coords):
    """Return True if tweets i and j are considered duplicates."""
    if typologies[i] != typologies[j]:
        return False
    if abs(timestamps[i] - timestamps[j]) > TIME_WINDOW:
        return False

    sim       = sim_matrix[i][j]
    dist      = distance_km(coords[i], coords[j])
    same_area = dist < GEO_RADIUS_KM

    return sim >= SIM_THRESHOLD or (same_area and sim >= SIM_GEO_THRESHOLD)


def group_duplicates(n, sim_matrix, timestamps, typologies, coords):
    """
    Groups duplicate tweets transitively.
    e.g. if A≈B and B≈C, then A, B, C are in the same group
    even if A and C are not directly similar.

    How it works: each tweet starts in its own group (parent[i] = i).
    When i and j are detected as duplicates, their groups are merged.
    At the end, all tweets in the same group share the same "representative".
    """
    parent = list(range(n))  # each tweet is its own representative at first

    def find(x):
        # Walk up to the representative of x's group
        while parent[x] != x:
            parent[x] = parent[parent[x]]  # shortcut to speed things up
            x = parent[x]
        return x

    def merge(x, y):
        # Merge the groups of x and y
        parent[find(x)] = find(y)

    for i in range(n):
        for j in range(i + 1, n):
            if are_duplicates(i, j, sim_matrix, timestamps, typologies, coords):
                merge(i, j)

    # Collect indices by group
    groups = defaultdict(list)
    for i in range(n):
        groups[find(i)].append(i)

    return [g for g in groups.values() if len(g) > 1]  # only keep actual groups


def pick_best_tweet(group, tweets):
    """
    Among a group of duplicates, keep the tweet with the highest importance score.
    Ties are broken by keeping the oldest (likely the primary source).
    """
    return max(
        group,
        key=lambda i: (tweets[i]["score"] or 0, -tweets[i]["ts"].timestamp())
    )


# ── Main function ─────────────────────────────────────────────────────────────
def delete_duplicates(dry_run=False):
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    try:
        cur.execute("""
            SELECT tweet_id,
                   COALESCE(summary_text, text),
                   created_at,
                   conflict_typology,
                   ST_Y(geom::geometry) AS lat,
                   ST_X(geom::geometry) AS lon,
                   importance_score
            FROM   tweets
            WHERE  created_at >= NOW() - INTERVAL '24 hours'
              AND  geom IS NOT NULL
            ORDER BY created_at DESC
        """)
        rows = cur.fetchall()

        if not rows:
            print("No recent tweets found.")
            return

        print(f"{len(rows)} tweets loaded.")

        tweets = [
            {
                "id":    r[0],
                "text":  r[1],
                "ts":    r[2],
                "typo":  r[3],
                "coord": (r[4], r[5]),
                "score": r[6],
            }
            for r in rows
        ]

        # Compute semantic similarities
        model      = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
        embeddings = model.encode([t["text"] for t in tweets], show_progress_bar=True)
        sim_matrix = cosine_similarity(embeddings)

        # Detect duplicate groups
        groups = group_duplicates(
            n          = len(tweets),
            sim_matrix = sim_matrix,
            timestamps = [t["ts"]    for t in tweets],
            typologies = [t["typo"]  for t in tweets],
            coords     = [t["coord"] for t in tweets],
        )

        print(f"{len(groups)} duplicate group(s) found.")

        # For each group: keep the best tweet, delete the rest
        to_delete = []
        for group in groups:
            idx_keep = pick_best_tweet(group, tweets)
            for idx in group:
                if idx != idx_keep:
                    to_delete.append(tweets[idx]["id"])
            print(f"  Group of {len(group)} → keep {tweets[idx_keep]['id']}, "
                  f"delete {[tweets[i]['id'] for i in group if i != idx_keep]}")

        print(f"{len(to_delete)} tweet(s) to delete.")

        if to_delete and not dry_run:
            cur.execute("DELETE FROM tweets WHERE tweet_id = ANY(%s)", (to_delete,))
            conn.commit()
            print("Deletion complete.")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate without deleting.")
    args = parser.parse_args()
    delete_duplicates(dry_run=args.dry_run)