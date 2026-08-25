"""
OSINT event collection and geolocation script
===============================================
...
"""

import requests
import psycopg2
import time
from rss_to_json import parse_to_json
from llm_geocode import extract_events_and_geoloc_batch, chunked, BATCH_SIZE as GEOCODE_BATCH_SIZE
import os
from dotenv import load_dotenv
from flag_db_duplicates import flag_duplicates
from llm_aggressor_extraction import generate_aggressor
from save_threat_snapshot import save_threat_snapshot
from nominatim_search import nominatim_geolocation_closest
from translate_tweet_text import translate_to_english
from llm_strait_state import save_strait_state
from llm_insert_topic import insert_topics
from llm_daily_summary import summarize_from_db
from llm_generate_trending_keywords import build_keywords
from location_correction import apply_correction
import subprocess
import token_tracker
import time
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

LLAMA_SERVER_PORT = 8081
LLAMA_SERVER_CMD = [
    "llama-server",
    "-hf", "unsloth/gemma-4-26B-A4B-it-qat-GGUF:UD-Q4_K_XL",
    "--port", str(LLAMA_SERVER_PORT),
    "-ngl", "999",
    "--n-cpu-moe", "20",
    "--ctx-size", "16000",
    "-fa", "on",
    "--cache-type-k", "q8_0",
    "--cache-type-v", "q8_0",
    "--reasoning", "off",
    "--no-mmproj"
]
LLAMA_SERVER_STARTUP_TIMEOUT = 300  

SOURCES = [
    "@GeoConfirmed", "@sentdefender", "@OSINTWarfare",
    "@Osinttechnical", "@Conflict_Radar", "@NOELreports",
    "@wartranslated","@sudanwarmonitor","@war_noir","@fabsenbln",
    "@khorasandiary", "@martinplaut","@BrantPhilip_","@sheehanj920",
    "@Intelynx","@Wamaps_news","@ADFmagazine","@mintelworld",
    "@99Dominik_", "@geo27752","@aamajnews_EN",
    "@PakDefence_","@Archer83Able","@SNAForce",
    "@Myanmar_Now_Eng","@sterrorwatch","@zarGEOINT","@neonhandrail",
    "@BabakTaghvaee1", "@avivector", "@Exilenova_plus"
]

# ==============================================================================
# SQL QUERIES
# ==============================================================================

SQL_GET_TWEET_IDS = "SELECT tweet_id FROM tweets"

SQL_INSERT_TWEET_FULL = """
    INSERT INTO public.tweets (
        tweet_id, created_at, tweet_url, username, text,
        location_accuracy, importance_score, conflict_typology,
        summary_text, nominatim_query, geom, location_source, is_delayed
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        CASE WHEN %s IS NOT NULL THEN ST_GeomFromText(%s, 4326) ELSE NULL END, %s, %s
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
# CONNECTION
# ==============================================================================

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database"""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


def start_llama_server():
    """Starts llama-server in the background and waits until it's ready to
    accept requests (model fully loaded), or raises after the timeout."""

    # Skip if a server is already running on that port
    health_check = subprocess.run(
        ["lsof", "-ti", f":{LLAMA_SERVER_PORT}"],
        capture_output=True, text=True
    )
    if health_check.stdout.strip():
        print(f"Serveur llama.cpp déjà actif sur le port {LLAMA_SERVER_PORT}, réutilisation.")
        return None

    print("Démarrage du serveur llama.cpp...")
    process = subprocess.Popen(
        LLAMA_SERVER_CMD,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    start_time = time.time()
    while time.time() - start_time < LLAMA_SERVER_STARTUP_TIMEOUT:
        try:
            resp = requests.get(f"http://localhost:{LLAMA_SERVER_PORT}/health", timeout=2)
            if resp.status_code == 200:
                print(f"Serveur llama.cpp prêt ({time.time() - start_time:.0f}s).")
                return process
        except requests.exceptions.RequestException:
            pass
        time.sleep(2)

    process.kill()
    raise RuntimeError(f"llama-server n'a pas démarré dans les {LLAMA_SERVER_STARTUP_TIMEOUT}s impartis.")

# ==============================================================================
# MAIN SCRIPT
# ==============================================================================

llama_process = start_llama_server()

conn = get_db_connection()
cur = conn.cursor()

# Preload existing tweet IDs to skip anything already stored
cur.execute(SQL_GET_TWEET_IDS)
tweet_in_db = [i[0] for i in cur.fetchall()]

directional_keywords = [
'Middle East',
'Eastern', 'Northern', 'Western', 'Southern',
'North', 'South', 'East', 'West'
]

# Phase 1 — Fetch and filter candidate tweets from every source's RSS feed.
# No LLM call happens here: we just decide which items are worth analyzing
# (not already stored, not a retweet/link-only/update post, GeoConfirmed rule).
candidates = []

for source in SOURCES:

    try:
        osint_json = parse_to_json(f"http://localhost:8080/{source[1:]}/rss", source)
        time.sleep(5)
        for item in osint_json["tweets"]:
            if item["id"] in tweet_in_db:
                continue

            if source == "@GeoConfirmed":
                if not item["description"].startswith("GeoConfirmed "):
                    continue

            tweet_text = item["title"]

            # Skip retweets, link-only posts, and update posts
            if tweet_text.startswith(("RT", "x.com", "Update")):
                continue

            candidates.append({**item, "tweet_text": tweet_text})

    except Exception as error:
        print(error)

print(f"{len(candidates)} tweet(s) candidat(s) à analyser.")

# Phase 2 — Batch the candidates through the geocoding LLM (BATCH_SIZE tweets per
# call instead of one call per tweet), then run the same per-tweet post-processing
# as before (translation, Nominatim refinement, DB insertion, images).
for batch in chunked(candidates, GEOCODE_BATCH_SIZE):
    llm_batch = [(str(item["id"]), item["tweet_text"]) for item in batch]

    try:
        results = extract_events_and_geoloc_batch(llm_batch)
    except Exception as e:
        print("LLM batch error:", e)
        results = {}

    for item in batch:
        result = results.get(str(item["id"]))

        if result is None:
            # Whole batch failed, or the LLM omitted this tweet from its response.
            # The tweet was never inserted, so it stays a candidate for the next run.
            print(f"[WARN] Pas de résultat LLM pour le tweet {item['id']}, retenté au prochain run.")
            continue

        tweet_text = item["tweet_text"]
        events = result.get("events", [])

        # No event extracted: store the tweet with minimal info and flag it as a duplicate
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
        is_delayed = event.get("is_delayed")

        # If the LLM's location isn't explicit, refine it via Nominatim
        if location_accuracy and location_accuracy != "explicit":
            nominatim_search = nominatim_geolocation_closest(nominatim_query, lat, lon)
            time.sleep(1)
            
            if isinstance(nominatim_search, list):
                lat, lon = nominatim_search
                location_source = "Nominatim"

            elif isinstance(nominatim_search, str) and nominatim_search.split(" ")[0] in directional_keywords:
                print("CORRECTION POINT", nominatim_search)
                correction = apply_correction(nominatim_search)
                if correction is not None:
                    lon, lat = correction
                    location_source = "LLM"

        geom_wkt = f"POINT({lon} {lat})" if lat and lon else None

        print(nominatim_query, geom_wkt, summary_text, location_source)

        # Store the fully processed tweet with its geolocation and metadata
        cur.execute(SQL_INSERT_TWEET_FULL, (
            item["id"], item["date"], item["link"], item["author"], tweet_text,
            location_accuracy, strategic_importance, typology,
            summary_text, nominatim_query, geom_wkt, geom_wkt, location_source, is_delayed
        ))
        conn.commit()

        # Store any attached images for this tweet
        for img in item["images"]:
            cur.execute(SQL_INSERT_IMAGE, (item["id"], img))
            conn.commit()



# Post-processing: dedupe, snapshot threats, extract aggressors, summarize, keywords
flag_duplicates()
save_threat_snapshot()
generate_aggressor()
save_strait_state()
insert_topics()
summarize_from_db()
build_keywords()

cur.execute(SQL_INSERT_DAILY_CONFLICTS)
conn.commit()

cur.close()
conn.close()

# Stop the local llama.cpp server (llama-server) to free VRAM once processing is done
try:
    if llama_process is not None:
        llama_process.terminate()
        llama_process.wait(timeout=30)
        print("Serveur llama.cpp arrêté (process démarré par ce script).")
    else:
        # We didn't start it (was already running) — stop it by port instead
        result = subprocess.run(
            ["lsof", "-ti", f":{LLAMA_SERVER_PORT}"],
            capture_output=True, text=True
        )
        pids = [pid for pid in result.stdout.strip().splitlines() if pid]
        if pids:
            subprocess.run(["kill", "-15", *pids])
            print(f"Serveur llama.cpp arrêté (port {LLAMA_SERVER_PORT}, PID {', '.join(pids)})")
        else:
            print(f"Aucun serveur llama.cpp trouvé sur le port {LLAMA_SERVER_PORT}")
except Exception as e:
    print("Impossible de stopper llama-server :", e)

print(token_tracker.summary())