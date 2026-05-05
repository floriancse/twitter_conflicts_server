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
SIM_THRESHOLD     = 0.82
SIM_GEO_THRESHOLD = 0.72
GEO_RADIUS_KM     = 50
TIME_WINDOW       = timedelta(hours=6)


# ── Fonctions utilitaires ─────────────────────────────────────────────────────
def distance_km(c1, c2):
    R = 6371.0
    lat1, lon1 = radians(c1[0]), radians(c1[1])
    lat2, lon2 = radians(c2[0]), radians(c2[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))


def are_duplicates(i, j, sim_matrix, timestamps, typologies, coords):
    if typologies[i] != typologies[j]:
        return False
    if abs(timestamps[i] - timestamps[j]) > TIME_WINDOW:
        return False

    sim       = sim_matrix[i][j]
    dist      = distance_km(coords[i], coords[j])
    same_area = dist < GEO_RADIUS_KM

    return sim >= SIM_THRESHOLD or (same_area and sim >= SIM_GEO_THRESHOLD)


def group_duplicates(n, sim_matrix, timestamps, typologies, coords):
    groups = defaultdict(list)

    for i in range(n):
        placed = False
        for rep, members in groups.items():
            if are_duplicates(i, rep, sim_matrix, timestamps, typologies, coords):
                members.append(i)
                placed = True
                break
        if not placed:
            groups[i].append(i)

    return groups


def pick_best_tweet(group, tweets):
    return max(
        group,
        key=lambda i: (tweets[i]["score"] or 0, -tweets[i]["ts"].timestamp())
    )


# ── Fonction principale ───────────────────────────────────────────────────────
def flag_duplicates(dry_run=False):
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

        # Détection de tous les groupes
        all_groups = group_duplicates(
            n          = len(tweets),
            sim_matrix = sim_matrix,
            timestamps = [t["ts"]    for t in tweets],
            typologies = [t["typo"]  for t in tweets],
            coords     = [t["coord"] for t in tweets],
        )

        duplicate_groups = {rep: members for rep, members in all_groups.items() if len(members) > 1}
        singleton_groups = {rep: members for rep, members in all_groups.items() if len(members) == 1}

        print(f"{len(duplicate_groups)} groupe(s) de doublons trouvé(s).")
        print(f"{len(singleton_groups)} tweet(s) isolé(s).")

        duplicate_ids  = []  
        singleton_ids  = []  
        kept_from_duplicates = []

        for rep, group in duplicate_groups.items():
            idx_keep = pick_best_tweet(group, tweets)
            kept_id  = tweets[idx_keep]["id"]
            dup_ids  = [tweets[i]["id"] for i in group if i != idx_keep]

            duplicate_ids.extend(dup_ids)
            singleton_ids.append(kept_id)
            kept_from_duplicates.append(kept_id)

        for rep, group in singleton_groups.items():
            singleton_ids.append(tweets[rep]["id"])

        print(f"kept_from_duplicates: {kept_from_duplicates}")

        if not dry_run:
            if duplicate_ids:
                cur.execute(
                    "UPDATE tweets SET is_duplicate = True WHERE tweet_id = ANY(%s)",
                    (duplicate_ids,)
                )
            if singleton_ids:
                cur.execute(
                    "UPDATE tweets SET is_duplicate = False WHERE tweet_id = ANY(%s)",
                    (singleton_ids,)
                )
            
            if kept_from_duplicates:
                cur.execute(
                    "UPDATE tweets SET verified = true WHERE tweet_id = ANY(%s)",
                    (kept_from_duplicates,)
                )
                
            conn.commit()
            print("Marquage terminé.")
        else:
            print("[dry-run] Aucune modification effectuée en base.")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    flag_duplicates()