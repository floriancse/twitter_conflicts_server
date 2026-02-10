"""
API REST FastAPI pour l'accès aux données OSINT géolocalisées
==============================================================

Cette API expose les données de tweets OSINT stockées dans PostgreSQL/PostGIS
via plusieurs endpoints permettant la visualisation cartographique et l'analyse.

Endpoints principaux :
- /api/twitter_conflicts/tweets.geojson : Tweets géolocalisés (format GeoJSON)
- /api/twitter_conflicts/authors : Liste des auteurs actifs
- /api/twitter_conflicts/important_tweets : Événements stratégiques (importance ≥ 4)
- /api/twitter_conflicts/random_tweets : Échantillon de tweets non géolocalisés
- /api/twitter_conflicts/disputed_area.geojson : Zones de conflit (polygones)

Configuration :
- Base de données : PostgreSQL avec extension PostGIS
- CORS : Activé pour développement local 
- Variables d'environnement : Chargées depuis fichier .env
"""

from fastapi import FastAPI, Response, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import psycopg2
import os
import json
import geojson
from datetime import datetime, timedelta
from typing import Optional, List

load_dotenv()

# Configuration de la connexion à la base de données PostgreSQL/PostGIS
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),          
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user": os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "disable"),      
}

app = FastAPI()

# Configuration CORS pour autoriser les requêtes depuis le frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5500",     # Serveur de développement local (Live Server VSCode)
        "http://localhost:5500",     # Alias localhost
        "*",                         # Tous les domaines (à restreindre en production)
    ],
    allow_credentials=False,         # Pas d'authentification pour cette API
    allow_methods=["*"],             # Toutes les méthodes HTTP autorisées
    allow_headers=["*"],             # Tous les headers autorisés
)

def get_db_connection():
    """
    Établit et retourne une connexion à la base de données PostgreSQL.
    
    Returns:
        psycopg2.connection: Objet de connexion à la base de données
    """
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )
    return conn


@app.get("/api/twitter_conflicts/disputed_area.geojson")
def get_disputed_area():
    """
    Retourne les zones de conflit/contestées en format GeoJSON.
    
    Utilise les fonctions PostGIS pour convertir les géométries PostgreSQL
    en GeoJSON standard compatible avec les bibliothèques cartographiques (Leaflet, Mapbox).
    
    Returns:
        Response: GeoJSON FeatureCollection contenant les polygones des zones disputées
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Construction du GeoJSON directement en SQL avec json_build_object et ST_AsGeoJSON
    cur.execute(
        """
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(
                json_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(geom)::json,
                    'properties', json_build_object(
                        'id', id,
                        'name', name
                    )
                )
            )
        )
        FROM public.disputed_area;
    """
    )

    geojson_data = cur.fetchone()[0]

    cur.close()
    conn.close()

    return Response(content=json.dumps(geojson_data), media_type="application/json")


@app.get("/api/twitter_conflicts/world_countries.geojson")
def get_disputed_area():
    """
    Retourne les pays en format GeoJSON.
    
    Utilise les fonctions PostGIS pour convertir les géométries PostgreSQL
    en GeoJSON standard compatible avec les bibliothèques cartographiques (Leaflet, Mapbox).
    
    Returns:
        Response: GeoJSON FeatureCollection contenant les polygones des zones disputées
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Construction du GeoJSON directement en SQL avec json_build_object et ST_AsGeoJSON
    cur.execute(
        """
        SELECT
            JSON_BUILD_OBJECT(
                'type',
                'FeatureCollection',
                'features',
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'type',
                        'Feature',
                        'geometry',
                        ST_ASGEOJSON (GEOM)::JSON,
                        'properties',
                        JSON_BUILD_OBJECT('id', OGC_FID, 'name', SOVEREIGNT)
                    )
                )
            )
        FROM
            PUBLIC.WORLD_COUNTRIES;
    """
    )

    geojson_data = cur.fetchone()[0]

    cur.close()
    conn.close()

    return Response(content=json.dumps(geojson_data), media_type="application/json")


@app.get("/api/twitter_conflicts/authors")
def get_authors(hours: int = 720):
    """
    Retourne la liste des auteurs distincts ayant publié des tweets sur une période donnée.
    
    Args:
        hours (int): Période en heures (par défaut 720h = 30 jours)
        
    Returns:
        dict: {"authors": ["@author1", "@author2", ...]}
        
    Utilisé pour alimenter les filtres de recherche dans l'interface utilisateur.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Requête avec intervalle dynamique basé sur NOW()
    cur.execute(
        """
        SELECT DISTINCT author
        FROM public.tweets
        WHERE date_published >= NOW() - INTERVAL '%s hours'
        ORDER BY author;
        """,
        (hours,)
    )

    authors = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()

    return {"authors": authors}


@app.get("/api/twitter_conflicts/tweets.geojson")
def get_tweets(hours: int = 24, q: Optional[str] = None, authors: Optional[str] = None):
    """
    Retourne les tweets géolocalisés en format GeoJSON avec filtrage avancé.
    
    Args:
        hours (int): Période temporelle en heures (par défaut 24h)
        q (str, optional): Recherche textuelle (ILIKE sur body et author)
        authors (str, optional): Liste d'auteurs séparés par virgules (ex: "@user1,@user2")
        
    Returns:
        Response: GeoJSON FeatureCollection avec les tweets et leurs métadonnées
        
    Exemple d'utilisation :
        /api/twitter_conflicts/tweets.geojson?hours=48&q=missile&authors=@GeoConfirmed
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Construction dynamique de la clause WHERE
    conditions = ["date_published >= NOW() - INTERVAL '%s hours'"]
    params = [hours]

    # Filtre de recherche textuelle (insensible à la casse)
    if q:
        conditions.append("(body ILIKE %s OR author ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])

    # Filtre par auteurs multiples
    if authors:
        author_list = [a.strip() for a in authors.split(',') if a.strip()]
        if author_list:
            placeholders = ','.join(['%s'] * len(author_list))
            conditions.append(f"author IN ({placeholders})")
            params.extend(author_list)

    where_clause = " AND ".join(conditions)

    # Requête SQL avec construction GeoJSON intégrée incluant les images
    query = f"""
        SELECT
            JSON_BUILD_OBJECT(
                'type', 'FeatureCollection',
                'features', JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(t.geom)::JSON,
                        'properties', JSON_BUILD_OBJECT(
                            'id',               t.id,
                            'url',              t.url,
                            'author',           t.author,
                            'date_published',   t.date_published,
                            'body',             t.body,
                            'accuracy',         t.accuracy,
                            'importance',       t.importance,
                            'typology',         t.typology,
                            'country_name',     wc.SOVEREIGNT,
                            'images', COALESCE(
                                (
                                    SELECT JSON_AGG(ti.image_url ORDER BY ti.image_url)
                                    FROM public.tweet_image ti
                                    WHERE ti.tweet_id = t.tweet_id
                                ),
                                '[]'::JSON
                            )
                        )
                    )
                )
            )
        FROM public.tweets t
        LEFT JOIN public.world_countries wc
            ON ST_Contains(wc.geom, t.geom)
        WHERE {where_clause};
    """

    cur.execute(query, params)

    # Gestion du cas sans résultats (GeoJSON vide valide)
    geojson_data = cur.fetchone()[0] or {
        "type": "FeatureCollection",
        "features": []
    }

    cur.close()
    conn.close()

    return Response(content=json.dumps(geojson_data), media_type="application/json")


@app.get("/api/twitter_conflicts/last_tweet_date")
def get_last_tweet_date():
    """
    Retourne la date et l'heure du dernier tweet enregistré dans la base.
    
    Returns:
        dict: {"last_date": "2026-02-06", "last_hour": "14:23:45"}
        
    Utilisé pour afficher la fraîcheur des données dans l'interface.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            MAX(DATE_PUBLISHED)::date as last_date, 
            MAX(DATE_PUBLISHED)::time as last_hour
        FROM
            public.TWEETS;
    """
    )

    get_date = cur.fetchone()
    cur.close()
    conn.close()

    return {"last_date": get_date[0], "last_hour": get_date[1]}


"""
NOUVEAU ENDPOINT À AJOUTER DANS TON FICHIER API (main.py ou api.py)
====================================================================
"""
@app.get("/api/twitter_conflicts/country_stats")
def get_country_stats(
    country_name: str,
    hours: int = Query(24, ge=1)
):
    conn = get_db_connection()
    cur = conn.cursor()

    # Déterminer l'intervalle
    if hours <= 24:
        interval_hours = 2
        interval_sql = '2 hours'
        bucket_format = 'YYYY-MM-DD HH24:MI'  # pour labels clairs si besoin
    elif hours <= 168:  # 7 jours
        interval_hours = 12
        interval_sql = '12 hours'
        bucket_format = 'YYYY-MM-DD HH24'
    else:  # 30 jours ou plus
        interval_hours = 24
        interval_sql = '1 day'
        bucket_format = 'YYYY-MM-DD'

    # ───────────────────────────────────────────────
    # 1. Date de fin = NOW()
    # 2. Date de début = NOW() - hours
    # ───────────────────────────────────────────────

    query = """
    WITH params AS (
        SELECT 
            NOW() - INTERVAL '%s hours' AS period_start,
            NOW() AS period_end,
            INTERVAL '%s' AS bucket_size
    ),
    all_buckets AS (
        SELECT 
            gs.time_bucket
        FROM params p,
        generate_series(
            date_trunc('hour', p.period_start),
            date_trunc('hour', p.period_end),
            p.bucket_size
        ) AS gs(time_bucket)
    ),
    filtered_tweets AS (
        SELECT 
            date_published,
            1 AS event_count
        FROM public.tweets t
        LEFT JOIN public.world_countries wc ON ST_Contains(wc.geom, t.geom)
        WHERE 
            wc.SOVEREIGNT = %s
            AND date_published >= (SELECT period_start FROM params)
            AND date_published <= (SELECT period_end FROM params)
    ),
    aggregated AS (
        SELECT 
            date_trunc(%s, ft.date_published) AS time_bucket,
            COUNT(*) AS total
        FROM filtered_tweets ft
        GROUP BY 1
    )
    SELECT 
        ab.time_bucket,
        COALESCE(agg.total, 0) AS total
    FROM all_buckets ab
    LEFT JOIN aggregated agg ON ab.time_bucket = agg.time_bucket
    ORDER BY ab.time_bucket ASC;
    """

    # Exécute avec le bon troncature selon interval
    cur.execute(query, (
        hours,
        interval_sql,
        country_name,
        interval_sql   # pour date_trunc dans aggregated
    ))

    results = cur.fetchall()

    data = []
    for row in results:
        data.append({
            "timestamp": row[0].isoformat() if row[0] else None,
            "total": int(row[1]),
        })

    cur.close()
    conn.close()

    return {
        "country": country_name,
        "period_hours": hours,
        "interval_hours": interval_hours,
        "data": data
    }


@app.get("/api/twitter_conflicts/country_info")
def get_country_info(
    country_name: str,
    hours: int = 24
):
    """
    Retourne les informations générales d'un pays pour une période donnée.
    
    Args:
        country_name (str): Nom du pays
        hours (int): Période en heures
        
    Returns:
        dict: Statistiques générales du pays
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT author) as unique_authors,
            MAX(date_published) as last_event_date
        FROM public.tweets t
        LEFT JOIN public.world_countries wc ON ST_Contains(wc.geom, t.geom)
        WHERE 
            wc.SOVEREIGNT = %s
            AND date_published >= NOW() - INTERVAL '%s hours';
    """
    
    cur.execute(query, (country_name, hours))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    return {
        "country": country_name,
        "period_hours": hours,
        "total_events":   result[0] if result else 0,
        "unique_authors": result[1] if result else 0,
        "last_event_date": result[2].isoformat() if result and result[2] else None
    }