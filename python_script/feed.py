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

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),           # fallback localhost
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user": os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "disable"),      # disable pour VPS local
}

def get_db_connection():
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )
    return conn
    
conn = get_db_connection()
cur = conn.cursor()
cur.execute("SELECT tweet_id from tweets")
tweet_in_db = [i[0] for i in cur.fetchall()]
sources = ["@GeoConfirmed", "@sentdefender","@OSINTWarfare","@Osinttechnical","@Conflict_Radar"]

accuracy_table = {
    "high":"Haute",
    "medium":"Moyenne",
    "low":"Basse"
}

for source in sources:
    osint_json = parse_to_json(f"http://localhost:8080/{source[1:]}/rss", source)
    
    for item in osint_json["tweets"]:
        if item["id"] in tweet_in_db:
            continue

        if source == "@GeoConfirmed":
            if not item["description"].startswith("GeoConfirmed "):
                continue

        desc = item["title"]

        if desc.startswith(("RT", "x.com","Update")):
            continue
        
        try:
            llm_to_geocode = extract_events_and_geoloc(desc)
        except Exception as e:
            print("LLM error:", e)
            continue

        events = llm_to_geocode.get("events", [])

        if not events:
            if len(desc) > 50:
                cur.execute("""
                INSERT INTO public.TWEETS (tweet_id, date_published, url, author, body) 
                VALUES (%s, %s, %s, %s, %s)
                """, (item["id"], item["date"], item["link"], item["author"], item["title"]))
                conn.commit()
            continue

        event = events[0]   

        lat = event.get("latitude")
        long = event.get("longitude")
        location = event.get("main_location")
        strategic_importance = event.get("strategic_importance")
        typology = event.get("typologie")

        if lat is not None and long is not None:
            geom_wkt = f"POINT ({long} {lat})"
        else:
            geom_wkt = None

        tweet_accuracy = accuracy_table[event["confidence"]]    
        cur.execute("""
        INSERT INTO public.TWEETS (tweet_id, date_published, url, author, body, accuracy, importance, typology, GEOM) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 
                CASE WHEN %s IS NOT NULL 
                THEN ST_GeomFromText(%s, 4326) 
                ELSE NULL END)
            """, 
        (item["id"], item["date"], item["link"], item["author"], item["title"], tweet_accuracy, strategic_importance, typology,  geom_wkt, geom_wkt))

        conn.commit()

cur.close()
conn.close()
get_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
print(get_time, "Scraping terminé")