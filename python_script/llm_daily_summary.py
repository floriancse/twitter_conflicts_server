"""
Script de résumé quotidien d'événements OSINT via LLM
======================================================
Pour chaque (date, zone géographique), récupère les événements depuis la DB,
les résume en 1-2 phrases via un LLM local, et insère le résultat dans DAILY_SUMMARIES.
"""

import json
import psycopg2
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user":     os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode":  os.getenv("DB_SSLMODE", "disable"),
}

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://localhost:11434/v1")
LLM_API_KEY  = os.getenv("LLM_API_KEY", "ollama")
LLM_MODEL    = os.getenv("LLM_MODEL", "mistral-small:24b")

client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)

# ==============================================================================
# REQUÊTES SQL
# ==============================================================================
SQL_GET_DATES = """
SELECT CREATED_AT::DATE
FROM TWEETS T
WHERE CREATED_AT::DATE < CURRENT_DATE
GROUP BY CREATED_AT::DATE
ORDER BY CREATED_AT::DATE DESC
LIMIT 1
"""

SQL_GET_AREAS = """
SELECT ENTITY_NAME
FROM WORLD_AREAS A
WHERE EXISTS (
    SELECT 1 FROM TWEETS T
    WHERE ST_CONTAINS(A.GEOM, T.GEOM)
    AND CREATED_AT::DATE = %s
)
"""

SQL_GET_SUMMARIES = """
SELECT SUMMARY_TEXT
FROM TWEETS T
LEFT JOIN WORLD_AREAS A ON ST_INTERSECTS(A.GEOM, T.GEOM)
WHERE ENTITY_NAME = %s
  AND SUMMARY_TEXT IS NOT NULL
  AND CREATED_AT::DATE = %s
ORDER BY CREATED_AT DESC
"""

SQL_INSERT_SUMMARY = """
INSERT INTO DAILY_SUMMARIES (SUMMARY_DATE, COUNTRY, SUMMARY_TEXT)
VALUES (%s, %s, %s)
ON CONFLICT (SUMMARY_DATE, COUNTRY) DO NOTHING
"""

# ==============================================================================
# LLM
# ==============================================================================
SYSTEM_PROMPT = """You are a concise military/geopolitical analyst.
Given a list of OSINT events for a specific country/region on a given day,
produce a summary of 1 to 2 sentences in English that captures the key developments.
Respond ONLY with a valid JSON object in this exact format, no markdown, no preamble:
{{"summary": "Votre résumé ici."}}"""

def summarize_events(country: str, events_text: str) -> str | None:
    """Envoie les événements au LLM et retourne le résumé texte."""
    if not events_text.strip():
        return None
    try:
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Country/Region: {country}\n\nEvents:\n{events_text}"},
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
            max_tokens=1024,
        )
        raw = response.choices[0].message.content.strip()
        data = json.loads(raw)
        return data.get("summary")
    except Exception as e:
        print(f"  [LLM ERROR] {country}: {e}")
        return None

# ==============================================================================
# CONNEXION DB
# ==============================================================================
def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )

# ==============================================================================
# MAIN
# ==============================================================================
def run_daily_summary():
    conn = get_db_connection()
    cur = conn.cursor()

    # 1. Récupérer les dates
    cur.execute(SQL_GET_DATES)
    dates = [row[0] for row in cur.fetchall()]

    inserted = 0
    skipped  = 0

    for date in dates:
        # 2. Récupérer les zones actives pour cette date
        cur.execute(SQL_GET_AREAS, (date,))
        areas = [row[0] for row in cur.fetchall()]

        for area in areas:
            # 3. Récupérer les événements de la zone
            cur.execute(SQL_GET_SUMMARIES, (area, date))
            events_text = " ".join(row[0] for row in cur.fetchall())

            if not events_text.strip():
                skipped += 1
                continue

            # 4. Résumé LLM
            summary = summarize_events(area, events_text)
            if not summary:
                skipped += 1
                continue

            print(f"  [{area}] → {summary}")

            # 5. Insert dans DAILY_SUMMARIES
            cur.execute(SQL_INSERT_SUMMARY, (date, area, summary))
            inserted += 1

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    run_daily_summary()