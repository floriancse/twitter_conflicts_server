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
from flag_db_duplicates import flag_duplicates
from llm_aggressor_extraction import generate_aggressor
from save_threat_snapshot import save_threat_snapshot
from nominatim_search import nominatim_geolocation
from translate_tweet_text import translate_to_english
from llm_strait_state import save_strait_state
from llm_insert_topic import insert_topics
from llm_daily_summary import summarize_from_db
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
    "@Osinttechnical", "@Conflict_Radar", "@NOELreports",
    "@wartranslated","@sudanwarmonitor","@war_noir","@fabsenbln",
    "@khorasandiary", "@martinplaut","@BrantPhilip_","@sheehanj920",
    "@Intelynx","@Wamaps_news","@ADFmagazine","@mintelworld",
    "@99Dominik_", "@geo27752","@aamajnews_EN",
    "@PakDefence_","@jacksonhinklle","@Archer83Able","@SNAForce",
    "@Myanmar_Now_Eng","@sterrorwatch","@zarGEOINT","@neonhandrail", "@EpicFuryMap",
    "@BabakTaghvaee1", "@avivector"
]

# ==============================================================================
# REQUÊTES SQL
# ==============================================================================

SQL_GET_TWEET_IDS = "SELECT tweet_id FROM tweets"

SQL_INSERT_TWEET_FULL = """
    INSERT INTO public.tweets (
        tweet_id, created_at, tweet_url, username, text,
        location_accuracy, importance_score, conflict_typology,
        summary_text, nominatim_query, geom, location_source
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        CASE WHEN %s IS NOT NULL THEN ST_GeomFromText(%s, 4326) ELSE NULL END, %s
    )
    ON CONFLICT (tweet_id) DO NOTHING
"""

SQL_INSERT_TWEET_MINIMAL = """
    INSERT INTO
        PUBLIC.TWEETS (TWEET_ID, CREATED_AT, TWEET_URL, USERNAME, TEXT, IS_DUPLICATE)    
        VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (tweet_id) DO NOTHING
"""

SQL_INSERT_IMAGE = """
    INSERT INTO
        PUBLIC.TWEET_IMAGES (TWEET_ID, IMAGE_URL)    
        VALUES (%s, %s)
"""

SQL_INSERT_DAILY_CONFLICTS = """
    INSERT INTO
        DAILY_CONFLICT_PAIRS (SNAPSHOT_DATE, COUNTRY_A, COUNTRY_B, UPDATED_AT)
    SELECT DISTINCT
        CURRENT_DATE,
        LEAST(AGGRESSOR, TARGET),
        GREATEST(AGGRESSOR, TARGET),
        NOW()
    FROM
        MILITARY_ACTIONS MA
        LEFT JOIN TWEETS T ON T.TWEET_ID = MA.TWEET_ID
    WHERE
        TARGET IS NOT NULL
        AND DATE(CREATED_AT) = CURRENT_DATE
    ON CONFLICT (SNAPSHOT_DATE, COUNTRY_A, COUNTRY_B) DO UPDATE
    SET
        UPDATED_AT = NOW();
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
    print(source)

    try:
        osint_json = parse_to_json(f"http://localhost:8080/{source[1:]}/rss", source)
    except Exception as error:
        print(error)

    for item in osint_json["tweets"]:
        if item["id"] in tweet_in_db:
            continue

        if source == "@GeoConfirmed":
            if not item["description"].startswith("GeoConfirmed "):
                continue

        tweet_text = item["title"]

        if tweet_text.startswith(("RT", "x.com", "Update")):
            continue

        try:
            llm_to_geocode = extract_events_and_geoloc(tweet_text)
        except Exception as e:
            print("LLM error:", e)
            continue

        if llm_to_geocode is None:
            print(tweet_text, item["link"])
            continue

        events = llm_to_geocode.get("events", [])
        
        if not events:
            cur.execute(SQL_INSERT_TWEET_MINIMAL,
                (item["id"], item["date"], item["link"], item["author"], tweet_text, 'true'))
            conn.commit()
            continue

        tweet_text = translate_to_english(tweet_text)
        event = events[0]

        lat = event.get("lat")
        lon = event.get("lon")
        strategic_importance = int(event.get("strategic_importance") or 0)
        typology = event.get("typology")
        summary_text = event.get("summary_text")
        nominatim_query = event.get("nominatim_query")
        location_accuracy = event.get("confidence")
        location_source = "LLM"

        if location_accuracy != "explicit":
            nominatim_search = nominatim_geolocation(nominatim_query)
            if nominatim_search:
                lat, lon = nominatim_search[0], nominatim_search[1]
                location_source = "Nominatim"

        geom_wkt = f"POINT({lon} {lat})" if lat and lon else None
        print(nominatim_query, geom_wkt, summary_text)

        cur.execute(SQL_INSERT_TWEET_FULL, (
            item["id"], item["date"], item["link"], item["author"], tweet_text,
            location_accuracy, strategic_importance, typology,
            summary_text, nominatim_query, geom_wkt, geom_wkt, location_source
        ))
        conn.commit()

        for img in item["images"]:
            cur.execute(SQL_INSERT_IMAGE, (item["id"], img))
            conn.commit()

flag_duplicates()
save_threat_snapshot()
generate_aggressor()
save_strait_state()
insert_topics()
summarize_from_db()

cur.execute(SQL_INSERT_DAILY_CONFLICTS)
conn.commit()

cur.close()
conn.close()

get_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())