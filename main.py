from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import psycopg2
import os
import json
import geojson
from datetime import datetime, timedelta
from typing import Optional, List

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),          
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user": os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "disable"),      
}

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5500",     
        "http://localhost:5500",     
        "*",                        
    ],
    allow_credentials=False,        
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
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
    conn = get_db_connection()
    cur = conn.cursor()

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


@app.get("/api/twitter_conflicts/authors")
def get_authors(hours: int = 720):
    """
    Retourne la liste des auteurs distincts pour une période donnée en heures
    Par défaut 720 heures = 30 jours
    """
    conn = get_db_connection()
    cur = conn.cursor()

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
    Retourne les tweets en GeoJSON pour une période donnée en heures
    Par défaut 24 heures = 1 jour
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Utilisation de NOW() - INTERVAL pour les dernières X heures
    conditions = ["date_published >= NOW() - INTERVAL '%s hours'"]
    params = [hours]

    if q:
        conditions.append("(body ILIKE %s OR author ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])

    if authors:
        author_list = [a.strip() for a in authors.split(',') if a.strip()]
        if author_list:
            placeholders = ','.join(['%s'] * len(author_list))
            conditions.append(f"author IN ({placeholders})")
            params.extend(author_list)

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(
                json_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(geom)::json,
                    'properties', json_build_object(
                        'id', id,
                        'url', url,
                        'author', author,
                        'date_published', date_published,
                        'body', body,
                        'accuracy', accuracy,
                        'importance', importance,
                        'typology', typology
                    )
                )
            )
        )
        FROM public.tweets
        WHERE {where_clause} and GEOM IS NOT NULL;
    """

    cur.execute(query, params)

    geojson_data = cur.fetchone()[0] or {
        "type": "FeatureCollection",
        "features": []
    }

    cur.close()
    conn.close()

    return Response(content=json.dumps(geojson_data), media_type="application/json")


@app.get("/api/twitter_conflicts/last_tweet_date")
def get_last_tweet_date():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            MAX(DATE_PUBLISHED)::date as last_date, MAX(DATE_PUBLISHED)::time as last_hour
        FROM
            public.TWEETS;
    """
    )

    get_date = cur.fetchone()
    cur.close()
    conn.close()

    return {"last_date": get_date[0], "last_hour": get_date[1]}


@app.get("/api/twitter_conflicts/random_tweets")
def get_random_tweets(hours: int = 24):
    """
    Retourne une liste de tweets aléatoires sans géométrie
    Par défaut sur les dernières 24 heures
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            ID,
            BODY,
            AUTHOR,
            DATE_PUBLISHED,
            URL
        FROM
            public.TWEETS
        WHERE
            DATE_PUBLISHED >= NOW() - INTERVAL '%s hours'
            AND LENGTH(BODY) BETWEEN 50 AND 200 
            AND GEOM IS NULL
        ORDER BY
            RANDOM()
        LIMIT
            5 
        """,
        (hours,)
    )

    tweets = cur.fetchall()
    cur.close()
    conn.close()

    formatted_tweets = []
    for tweet in tweets:
        formatted_tweets.append({
            "id": tweet[0],
            "body": tweet[1],
            "author": tweet[2],
            "date_published": tweet[3].isoformat(),
            "url": tweet[4]
        })

    return {"tweets": formatted_tweets}


@app.get("/api/twitter_conflicts/important_tweets")
def get_important_tweets(hours: int = 24):
    """
    Retourne la liste des tweets importants sur les dernières heures
    Par défaut sur les dernières 24 heures
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            ID,
            BODY,
            AUTHOR,
            DATE_PUBLISHED,
            URL,
            ST_X (GEOM) AS LONG,
            ST_Y (GEOM) AS LAT
        FROM
            TWEETS
        WHERE
            IMPORTANCE::INT >= 4
            AND DATE_PUBLISHED >= NOW() - INTERVAL '%s hours'
        ORDER BY
            DATE_PUBLISHED DESC
        """,
        (hours,)
    )

    tweets = cur.fetchall()
    cur.close()
    conn.close()

    formatted_tweets = []
    for tweet in tweets:
        formatted_tweets.append({
            "id": tweet[0],
            "body": tweet[1],
            "author": tweet[2],
            "date_published": tweet[3].isoformat(),
            "url": tweet[4],
            "long": tweet[5],
            "lat": tweet[6],
        })

    return {"tweets": formatted_tweets}