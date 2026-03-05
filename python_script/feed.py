"""
Script de collecte et géolocalisation d'événements OSINT
=========================================================
...
"""

import json
import requests
import psycopg2
import re
import time
from rss_to_json import parse_to_json
from llm_geocode import extract_events_and_geoloc
import os
from dotenv import load_dotenv
from time import gmtime, strftime
from delete_db_duplicates import delete_dup_rows
from llm_aggressor_extraction import generate_aggressor
from llm_daily_summary import run_daily_summary

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

SOURCES = [
    "@GeoConfirmed", "@sentdefender", "@OSINTWarfare",
    "@Osinttechnical", "@Conflict_Radar", "@Globalsurv", "@NOELreports"
]

# ==============================================================================
# REQUÊTES SQL
# ==============================================================================

SQL_GET_TWEET_IDS = "SELECT tweet_id FROM tweets"

SQL_REFRESH_TENSION_MV = "REFRESH MATERIALIZED VIEW tension_index_mv"

SQL_INSERT_TWEET_FULL = """
    INSERT INTO public.tweets (
        tweet_id, created_at, tweet_url, username, text,
        location_accuracy, importance_score, conflict_typology,
        summary_text, location_name, geom
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        CASE WHEN %s IS NOT NULL THEN ST_GeomFromText(%s, 4326) ELSE NULL END
    )
    ON CONFLICT (tweet_id) DO NOTHING
"""

SQL_INSERT_TWEET_MINIMAL = """
    INSERT INTO
        PUBLIC.TWEETS (TWEET_ID, CREATED_AT, TWEET_URL, USERNAME, TEXT)    
        VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (tweet_id) DO NOTHING
"""

SQL_INSERT_IMAGE = """
    INSERT INTO
        PUBLIC.TWEET_IMAGES (TWEET_ID, IMAGE_URL)    
        VALUES (%s, %s)
"""

# ==============================================================================
# CONNEXION
# ==============================================================================

def get_db_connection():
    """Établit et retourne une connexion à la base de données PostgreSQL"""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )

# ==============================================================================
# SCRIPT PRINCIPAL
# ==============================================================================

conn = get_db_connection()
cur = conn.cursor()

cur.execute(SQL_GET_TWEET_IDS)
tweet_in_db = [i[0] for i in cur.fetchall()]

for source in SOURCES:
    osint_json = parse_to_json(f"http://localhost:8080/{source[1:]}/rss", source)

    for item in osint_json["tweets"]:
        if item["id"] in tweet_in_db:
            continue

        if source == "@GeoConfirmed":
            if not item["description"].startswith("GeoConfirmed "):
                continue

        desc = item["title"]

        if desc.startswith(("RT", "x.com", "Update")):
            continue

        try:
            llm_to_geocode = extract_events_and_geoloc(desc)
        except Exception as e:
            print("LLM error:", e)
            continue

        if llm_to_geocode is None:
            print(desc, item["link"])
            continue

        events = llm_to_geocode.get("events", [])

        if not events:
            if len(desc) > 50:
                cur.execute(SQL_INSERT_TWEET_MINIMAL,
                    (item["id"], item["date"], item["link"], item["author"], item["title"]))
                conn.commit()
            continue

        event = events[0]

        lat               = event.get("lat")
        lon               = event.get("lon")
        strategic_importance = int(event.get("strategic_importance"))
        typology          = event.get("typology")
        summary_text      = event.get("summary_text")
        location_name     = event.get("location_name")
        location_accuracy = event.get("confidence")
        geom_wkt          = f"POINT({lon} {lat})" if lat and lon else None

        print(location_name, geom_wkt, summary_text)

        cur.execute(SQL_INSERT_TWEET_FULL, (
            item["id"], item["date"], item["link"], item["author"], item["title"],
            location_accuracy, strategic_importance, typology,
            summary_text, location_name, geom_wkt, geom_wkt
        ))
        conn.commit()

        for img in item["images"]:
            cur.execute(SQL_INSERT_IMAGE, (item["id"], img))
            conn.commit()

cur.execute(SQL_REFRESH_TENSION_MV)
conn.commit()

delete_dup_rows()
generate_aggressor()
run_daily_summary()

cur.close()
conn.close()

get_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())