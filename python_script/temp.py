import psycopg2
import os
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME", "twitter_conflicts"),
    "user":     os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
}

def backfill_threat_history(start_date: date, end_date: date):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        current = start_date
        while current <= end_date:
            cur.execute("""
                INSERT INTO country_threat_history (
                    country,
                    threat_score,
                    event_count,
                    attacks_launched,
                    attacks_received,
                    threat_level,
                    raw_score,
                    max_severity,
                    snapshot_at,
                    snapshot_date
                )
                SELECT
                    country,
                    threat_score,
                    event_count,
                    attacks_launched,
                    attacks_received,
                    threat_level,
                    raw_score,
                    max_severity,
                    snapshot_at,
                    snapshot_date
                FROM compute_threat_scores(%s)

                ON CONFLICT (country, snapshot_date)
                DO UPDATE SET
                    threat_score     = EXCLUDED.threat_score,
                    event_count      = EXCLUDED.event_count,
                    attacks_launched = EXCLUDED.attacks_launched,
                    attacks_received = EXCLUDED.attacks_received,
                    threat_level     = EXCLUDED.threat_level,
                    raw_score        = EXCLUDED.raw_score,
                    max_severity     = EXCLUDED.max_severity,
                    snapshot_at      = EXCLUDED.snapshot_at;
            """, (current,))

            print(f"{current} — {cur.rowcount} pays insérés/mis à jour")
            current += timedelta(days=1)

        conn.commit()
        print("Backfill terminé ✓")

    except Exception as e:
        print(f"Erreur : {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # Adapter les dates selon la plage de tes tweets
    backfill_threat_history(
        start_date=date(2026, 2, 25),
        end_date=date(2026, 3, 12)   # la veille d'aujourd'hui
    )