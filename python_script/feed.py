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
from aggressor_extraction import generate_aggressor

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

SQL_GET_PROCESSED_ACTIONS = "SELECT tweet_id FROM MILITARY_ACTIONS"

SQL_REFRESH_TENSION_MV = "REFRESH MATERIALIZED VIEW tension_index_mv"

SQL_GET_RECENT_TWEETS_FOR_DEDUP = """
    SELECT tweet_id, summary_text, text, created_at, conflict_typology,
           ST_Y(geom::geometry) AS lat,
           ST_X(geom::geometry)  AS lon
    FROM   tweets
    WHERE  created_at >= NOW() - INTERVAL '24 hours'
      AND  geom IS NOT NULL
    ORDER BY created_at DESC
"""

SQL_GET_MIL_TWEETS = """
    SELECT tweet_id,
           created_at::DATE,
           summary_text,
           location_name,
           ST_X(geom) AS lon,
           ST_Y(geom) AS lat
    FROM   tweets
    WHERE  conflict_typology = 'MIL'
      AND  geom IS NOT NULL
      AND  summary_text IS NOT NULL
      AND  created_at >= NOW() - INTERVAL '24 hours'
    ORDER BY created_at DESC
"""

SQL_GET_CAPITALS = """
    SELECT a.entity_name,
           c.name,
           ST_X(c.geom) AS lon,
           ST_Y(c.geom) AS lat
    FROM   world_areas   a
    LEFT JOIN world_capitals c ON ST_Intersects(a.geom, c.geom)
    WHERE  c.geom IS NOT NULL
"""

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
    INSERT INTO public.tweets (tweet_id, created_at, tweet_url, username, text)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (tweet_id) DO NOTHING
"""

SQL_INSERT_IMAGE = """
    INSERT INTO public.tweet_images (tweet_id, image_url)
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

cur.execute(SQL_GET_RECENT_TWEETS_FOR_DEDUP)
rows = cur.fetchall()
delete_dup_rows(rows, cur, conn)

cur.execute(SQL_GET_MIL_TWEETS)
tweets = cur.fetchall()

cur.execute(SQL_GET_CAPITALS)
country_dict = {
    row[0]: [row[1], (row[2], row[3])]
    for row in cur.fetchall()
}

cur.execute(SQL_GET_PROCESSED_ACTIONS)
already_processed = {row[0] for row in cur.fetchall()}

generate_aggressor(cur, conn, tweets, already_processed, country_dict)

cur.close()
conn.close()

get_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())