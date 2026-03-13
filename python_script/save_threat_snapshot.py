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

def save_threat_snapshot():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

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
                NOW(),
                CURRENT_DATE
            FROM v_country_threat_score

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
        """)

        row_count = cur.rowcount
        conn.commit()
        print(f"Snapshot threat enregistrée : {row_count} pays insérés/mis à jour")

    except Exception as e:
        print(f"Erreur lors du snapshot : {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    save_threat_snapshot()