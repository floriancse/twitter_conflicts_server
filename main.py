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

from fastapi import FastAPI, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool
import os
import json
import geojson
from datetime import datetime, timedelta
from typing import Optional, List
import gzip
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import contextmanager

load_dotenv()

# Configuration de la connexion à la base de données PostgreSQL/PostGIS
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),          
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "twitter_conflicts"),
    "user": os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "disable"),
    # Keepalives pour éviter les coupures réseau OVH
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 5,
    "keepalives_count": 5,
}

# Pool de connexions : min 2, max 15 connexions simultanées
connection_pool = pool.ThreadedConnectionPool(
    minconn=2,
    maxconn=15,
    **DB_CONFIG
)

@contextmanager
def get_db():
    """
    Context manager qui emprunte une connexion du pool et la restitue
    automatiquement à la fin du bloc, même en cas d'exception.

    Usage :
        with get_db() as conn:
            cur = conn.cursor()
            ...
    """
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)


app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Configuration CORS pour autoriser les requêtes depuis le frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5500",                 
        "http://localhost:5500",
        "http://localhost:3000",                  
        "https://floriancse.github.io",          
    ],
    allow_credentials=False,       
    allow_methods=["*"],            
    allow_headers=["*"],           
)


@app.get("/api/twitter_conflicts/disputed_area.geojson")
def get_disputed_area():
    """
    Retourne les zones de conflit/contestées en format GeoJSON.
    
    Utilise les fonctions PostGIS pour convertir les géométries PostgreSQL
    en GeoJSON standard compatible avec les bibliothèques cartographiques (Leaflet, Mapbox).
    
    Returns:
        Response: GeoJSON FeatureCollection contenant les polygones des zones disputées
    """
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(
                    json_build_object(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(ST_Simplify(geom, 0.01), 4)::JSON,
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

    return Response(content=json.dumps(geojson_data), media_type="application/json")


@app.get("/api/twitter_conflicts/world_areas.geojson")
def get_world_areas():
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT JSON_BUILD_OBJECT(
                'type', 'FeatureCollection',
                'features', JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(ST_Simplify(geom, 0.01), 4)::JSON,
                        'properties', JSON_BUILD_OBJECT('id', id, 'name', "NAME_FR")
                    )
                )
            )
            FROM PUBLIC.world_areas;
        """)

        geojson_data = cur.fetchone()[0]
        cur.close()

    compact_geojson = json.dumps(
        geojson_data,
        separators=(',', ':'),
        ensure_ascii=False
    )

    return Response(
        content=compact_geojson,
        media_type="application/json"
    )


@app.get("/api/twitter_conflicts/authors")
def get_authors(
    start_date: datetime = Query(..., description="Date de début (ISO 8601, ex: 2026-02-14T00:00:00Z)"),
    end_date: datetime = Query(..., description="Date de fin (ISO 8601, ex: 2026-02-15T23:59:59Z)")
):
    """
    Retourne la liste des auteurs distincts ayant publié des tweets sur une période donnée.
    
    Args:
        start_date (datetime): Date de début (ISO 8601 avec timezone) - OBLIGATOIRE
        end_date (datetime): Date de fin (ISO 8601 avec timezone) - OBLIGATOIRE
        
    Returns:
        dict: {"authors": ["@author1", "@author2", ...]}
    """
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT DISTINCT author
            FROM public.tweets
            WHERE date_published >= %s AND date_published <= %s
            ORDER BY author;
            """,
            (start_date, end_date)
        )

        authors = [row[0] for row in cur.fetchall()]
        cur.close()

    return {"authors": authors}


@app.get("/api/twitter_conflicts/tweets.geojson")
def get_tweets(
    start_date: datetime = Query(..., description="Date de début (ISO 8601, ex: 2026-02-14T00:00:00Z)"),
    end_date: datetime = Query(..., description="Date de fin (ISO 8601, ex: 2026-02-15T23:59:59Z)"),
    q: Optional[str] = None,
    authors: Optional[str] = None,
    area: Optional[str] = None,         
    format: str = "geojson",                  
    sort: str = "date_desc",                
    page: int = 1,
    size: int = 50
):
    """
    Retourne les tweets géolocalisés en format GeoJSON avec filtrage avancé.
    
    Args:
        start_date (datetime): Date de début - OBLIGATOIRE
        end_date (datetime): Date de fin - OBLIGATOIRE
        q (str, optional): Recherche textuelle
        authors (str, optional): Liste d'auteurs séparés par virgules
        area (str, optional): Nom de la zone géographique
        
    Returns:
        Response: GeoJSON FeatureCollection
    """
    conditions = ["date_published >= %s AND date_published <= %s"]
    params = [start_date, end_date]

    if q:
        conditions.append("(body ILIKE %s OR author ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])

    if authors:
        author_list = [a.strip() for a in authors.split(',') if a.strip()]
        if author_list:
            placeholders = ','.join(['%s'] * len(author_list))
            conditions.append(f"author IN ({placeholders})")
            params.extend(author_list)

    if area:
        conditions.append("""wa."NAME_FR" = %s""")
        params.append(area)
        
    where_clause = " AND ".join(conditions)

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
                            'area_name',        wa."NAME_FR",
                            'images', COALESCE(
                                (
                                    SELECT JSON_AGG(ti.image_url ORDER BY ti.image_url)
                                    FROM public.tweets_image ti
                                    WHERE ti.tweet_id = t.tweet_id
                                ),
                                '[]'::JSON
                            )
                        )
                    )
                )
            )
        FROM public.tweets t
        LEFT JOIN public.world_areas wa
            ON ST_Contains(wa.geom, t.geom)
        WHERE {where_clause};
    """

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(query, params)

        geojson_data = cur.fetchone()[0] or {
            "type": "FeatureCollection",
            "features": []
        }
        cur.close()

    return Response(content=json.dumps(geojson_data), media_type="application/json")
    

@app.get("/api/twitter_conflicts/tension_index")
def get_tension_index(
    area: Optional[str] = None,         
):
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT *
            FROM TENSION_INDEX_MV
            WHERE COUNTRY = %s
            """,
            (area, )
        )

        try: 
            result = cur.fetchone()
            country = result[0]
            tension_score = result[1]
            niveau_tension = result[2]
            evenements_json = result[3]
            cur.close()

            return {
                "country": country,
                "tension_score": int(tension_score),
                "niveau_tension": niveau_tension,
                "evenements": evenements_json
            }
            
        except Exception:
            cur.close()
            return None