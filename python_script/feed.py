"""
Script de collecte et géolocalisation d'événements OSINT
=========================================================

Ce script collecte des tweets depuis plusieurs sources OSINT spécialisées dans les conflits,
extrait les événements géolocalisés via LLM, et stocke les données dans une base PostgreSQL/PostGIS.

Workflow :
1. Récupère les flux RSS de sources OSINT (GeoConfirmed, Sentdefender, etc.)
2. Filtre les tweets déjà en base de données
3. Utilise un LLM pour extraire les événements et coordonnées géographiques
4. Insère les tweets et leurs métadonnées géospatiales dans PostgreSQL

Dépendances :
- PostgreSQL avec extension PostGIS pour le stockage géospatial
- Serveur RSS local
- Module LLM pour la géolocalisation (llm_geocode)
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

load_dotenv()

# Configuration de la connexion à la base de données PostgreSQL
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),           
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user": os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "disable"),      
}

def get_db_connection():
    """Établit et retourne une connexion à la base de données PostgreSQL"""
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )
    return conn

# Initialisation de la connexion et récupération des tweets existants
conn = get_db_connection()
cur = conn.cursor()
cur.execute("SELECT tweet_id from tweets")
tweet_in_db = [i[0] for i in cur.fetchall()]

# Liste des sources OSINT à scraper
sources = ["@GeoConfirmed", "@sentdefender","@OSINTWarfare","@Osinttechnical","@Conflict_Radar","@WarMonitor3"]

# Mapping de la confiance LLM vers les valeurs françaises de la BDD
accuracy_table = {
    "high":"Haute",
    "medium":"Moyenne",
    "low":"Basse"
}

# Boucle principale : traitement de chaque source OSINT
for source in sources:
    # Récupération et parsing du flux RSS de la source
    osint_json = parse_to_json(f"http://localhost:8080/{source[1:]}/rss", source)
    
    for item in osint_json["tweets"]:
        # Skip les tweets déjà présents en base
        if item["id"] in tweet_in_db:
            continue

        # Filtre spécifique pour GeoConfirmed : ne garder que les tweets confirmés
        if source == "@GeoConfirmed":
            if not item["description"].startswith("GeoConfirmed "):
                continue

        desc = item["title"]

        # Filtrage des retweets, updates et liens simples
        if desc.startswith(("RT", "x.com","Update")):
            continue
        
        # Extraction des événements et géolocalisation via LLM
        try:
            llm_to_geocode = extract_events_and_geoloc(desc)
        except Exception as e:
            print("LLM error:", e)
            continue

        events = llm_to_geocode.get("events", [])

        # Si aucun événement détecté, insertion basique du tweet (si assez long)
        if not events:
            if len(desc) > 50:
                cur.execute("""
                INSERT INTO public.TWEETS (tweet_id, date_published, url, author, body) 
                VALUES (%s, %s, %s, %s, %s)
                """, (item["id"], item["date"], item["link"], item["author"], item["title"]))
                conn.commit()
            continue

        # Traitement du premier événement détecté
        event = events[0]   

        # Extraction des métadonnées de l'événement
        lat = event.get("latitude")
        long = event.get("longitude")
        location = event.get("main_location")
        strategic_importance = event.get("strategic_importance")
        typology = event.get("typologie")

        # Construction de la géométrie PostGIS (format WKT)
        if lat is not None and long is not None:
            geom_wkt = f"POINT ({long} {lat})"
        else:
            geom_wkt = None

        # Conversion de la confiance en français
        tweet_accuracy = accuracy_table[event["confidence"]]
        
        # Insertion du tweet avec toutes ses métadonnées géospatiales
        cur.execute("""
        INSERT INTO public.TWEETS (tweet_id, date_published, url, author, body, accuracy, importance, typology, GEOM) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 
                CASE WHEN %s IS NOT NULL 
                THEN ST_GeomFromText(%s, 4326) 
                ELSE NULL END)
            """, 
        (item["id"], item["date"], item["link"], item["author"], item["title"], tweet_accuracy, strategic_importance, typology,  geom_wkt, geom_wkt))
        conn.commit()

        for img in item["images"]:
            cur.execute("""
            INSERT INTO public.tweet_image (tweet_id, image_url) 
            VALUES (%s, %s)
            """, (item["id"], img))
            conn.commit()
            
# Fermeture propre des connexions
cur.close()
conn.close()

# Log de fin d'exécution
get_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
print(get_time, "Scraping terminé")