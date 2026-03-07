import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME", "twitter_conflicts"),
    "user":     os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
}

def save_tension_snapshot():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO tension_score_history (country, snapshot_at, tension_score, tension_level)
        SELECT
            country,
            CURRENT_DATE,
            tension_score,
            tension_level
        FROM tension_index_mv
        ON CONFLICT (country, snapshot_at) DO UPDATE
            SET tension_score = EXCLUDED.tension_score,
                tension_level = EXCLUDED.tension_level;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Snapshot tension enregistré pour {conn}")

if __name__ == "__main__":
    save_tension_snapshot()