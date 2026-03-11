import os
import psycopg2
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import timedelta
from math import radians, sin, cos, sqrt, atan2
from collections import defaultdict

load_dotenv()

# ── Connexion à la base de données ────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user":     os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
}

# ── Seuils ────────────────────────────────────────────────────────────────────
SIM_THRESHOLD     = 0.82   # similarité sémantique seule → doublon
SIM_GEO_THRESHOLD = 0.72   # seuil abaissé si les tweets sont dans la même zone
GEO_RADIUS_KM     = 50    # rayon en km pour considérer deux tweets dans la "même zone"
TIME_WINDOW       = timedelta(hours=24)


# ── Fonctions utilitaires ─────────────────────────────────────────────────────
def distance_km(c1, c2):
    """Distance en km entre deux points (lat, lon)."""
    R = 6371.0
    lat1, lon1 = radians(c1[0]), radians(c1[1])
    lat2, lon2 = radians(c2[0]), radians(c2[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))


def are_duplicates(i, j, sim_matrix, timestamps, typologies, coords):
    """Retourne True si les tweets i et j sont considérés comme doublons."""
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
    Regroupe les tweets doublons de façon transitive.
    Ex : si A≈B et B≈C, alors A, B et C sont dans le même groupe
    même si A et C ne sont pas directement similaires.

    Fonctionnement : chaque tweet démarre dans son propre groupe (parent[i] = i).
    Quand i et j sont détectés comme doublons, leurs groupes sont fusionnés.
    À la fin, tous les tweets du même groupe partagent le même "représentant".
    """
    parent = list(range(n))  # chaque tweet est son propre représentant au départ

    def find(x):
        # Remonte jusqu'au représentant du groupe de x
        while parent[x] != x:
            parent[x] = parent[parent[x]]  # raccourci pour accélérer
            x = parent[x]
        return x

    def merge(x, y):
        # Fusionne les groupes de x et y
        parent[find(x)] = find(y)

    for i in range(n):
        for j in range(i + 1, n):
            if are_duplicates(i, j, sim_matrix, timestamps, typologies, coords):
                merge(i, j)

    # Regroupe les indices par groupe
    groups = defaultdict(list)
    for i in range(n):
        groups[find(i)].append(i)

    return [g for g in groups.values() if len(g) > 1]  # ne garde que les vrais groupes


def pick_best_tweet(group, tweets):
    """
    Parmi un groupe de doublons, conserve le tweet avec le score d'importance le plus élevé.
    En cas d'égalité, on garde le plus ancien (probablement la source primaire).
    """
    return max(
        group,
        key=lambda i: (tweets[i]["score"] or 0, -tweets[i]["ts"].timestamp())
    )


# ── Fonction principale ───────────────────────────────────────────────────────
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
            print("Aucun tweet récent trouvé.")
            return

        print(f"{len(rows)} tweets chargés.")

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

        # Calcul des similarités sémantiques
        model      = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
        embeddings = model.encode([t["text"] for t in tweets], show_progress_bar=True)
        sim_matrix = cosine_similarity(embeddings)

        # Détection des groupes de doublons
        groups = group_duplicates(
            n          = len(tweets),
            sim_matrix = sim_matrix,
            timestamps = [t["ts"]    for t in tweets],
            typologies = [t["typo"]  for t in tweets],
            coords     = [t["coord"] for t in tweets],
        )

        print(f"{len(groups)} groupe(s) de doublons trouvé(s).")

        # Pour chaque groupe : on garde le meilleur tweet, on supprime les autres
        to_delete = []
        for group in groups:
            idx_keep = pick_best_tweet(group, tweets)
            for idx in group:
                if idx != idx_keep:
                    to_delete.append(tweets[idx]["id"])
            print(f"  Groupe de {len(group)} → on garde {tweets[idx_keep]['id']}, "
                  f"on supprime {[tweets[i]['id'] for i in group if i != idx_keep]}")

        print(f"{len(to_delete)} tweet(s) à supprimer.")

        if to_delete and not dry_run:
            cur.execute("DELETE FROM tweets WHERE tweet_id = ANY(%s)", (to_delete,))
            conn.commit()
            print("Suppression terminée.")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulation sans suppression.")
    args = parser.parse_args()
    delete_duplicates(dry_run=args.dry_run)