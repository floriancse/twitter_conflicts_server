"""
API REST FastAPI pour l'accès aux données OSINT géolocalisées
==============================================================

Cette API expose les données de tweets OSINT stockées dans PostgreSQL/PostGIS
via plusieurs endpoints permettant la visualisation cartographique et l'analyse.

Endpoints principaux :
- /api/twitter_conflicts/tweets.geojson : Tweets géolocalisés (format GeoJSON)
- /api/twitter_conflicts/usernames : Liste des auteurs actifs
- /api/twitter_conflicts/important_tweets : Événements stratégiques (importance_score ≥ 4)
- /api/twitter_conflicts/random_tweets : Échantillon de tweets non géolocalisés
- /api/twitter_conflicts/disputed_areas.geojson : Zones de conflit (polygones)

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
                        'properties', JSON_BUILD_OBJECT('id', id, 'name', entity_name)
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


@app.get("/api/twitter_conflicts/current_frontline.geojson")
def get_current_frontline():
    """
    """
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT JSON_AGG(
                JSON_BUILD_OBJECT(
                    'aggressor', AGGRESSOR,
                    'target', TARGET,
                    'intersection', ST_ASGEOJSON(geom.geom)::json
                )
            ) AS result
            FROM (
                SELECT
                    AGGRESSOR,
                    TARGET,
                    ST_LineMerge(ST_CollectionExtract(ST_INTERSECTION(A.GEOM, B.GEOM), 2)) AS INTERSECTION_GEOM
                FROM
                    MILITARY_ACTIONS M
                    LEFT JOIN WORLD_AREAS A ON M.AGGRESSOR = A.ENTITY_NAME
                    LEFT JOIN WORLD_AREAS B ON M.TARGET = B.ENTITY_NAME
                    LEFT JOIN TWEETS T ON M.TWEET_ID = T.TWEET_ID
                WHERE
                    TARGET IS NOT NULL
                    AND ST_INTERSECTS(A.GEOM, B.GEOM)
                    AND CREATED_AT >= NOW() - INTERVAL '14 days'
                GROUP BY
                    AGGRESSOR,
                    TARGET,
                    ST_INTERSECTION(A.GEOM, B.GEOM)
            ) sub,
            LATERAL ST_DUMP(INTERSECTION_GEOM) AS geom;
        """
        )

        geojson_data = cur.fetchone()[0]
        cur.close()

    return Response(content=json.dumps(geojson_data), media_type="application/json")


@app.get("/api/twitter_conflicts/shipping_lanes.geojson")
def get_shipping_lanes():
    """
    """
    with get_db() as conn:
        cur = conn.cursor()

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
                            ST_ASGEOJSON (ST_SIMPLIFY (GEOM, 0.01), 4)::JSON,
                            'properties',
                            JSON_BUILD_OBJECT('id', ID, 'type', TYPE)
                        )
                    )
                )
            FROM
                SHIPPING_LANES
            WHERE
                TYPE IN ('Major', 'Middle')
        """
        )

        geojson_data = cur.fetchone()[0]
        cur.close()

    return Response(content=json.dumps(geojson_data), media_type="application/json")


@app.get("/api/twitter_conflicts/chokepoints.geojson")
def get_checkpoints():
    """
    """
    with get_db() as conn:
        cur = conn.cursor()

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
                            ST_ASGEOJSON (ST_SIMPLIFY (GEOM, 0.01), 4)::JSON,
                            'properties',
                            JSON_BUILD_OBJECT('id', ID, 'portname', PORTNAME)
                        )
                    )
                )
            FROM
                CHOKEPOINTS
        """
        )

        geojson_data = cur.fetchone()[0]
        cur.close()

    return Response(content=json.dumps(geojson_data))


@app.get("/api/twitter_conflicts/usernames")
def get_usernames(
    start_date: datetime = Query(..., description="Date de début (ISO 8601, ex: 2026-02-14T00:00:00Z)"),
    end_date: datetime = Query(..., description="Date de fin (ISO 8601, ex: 2026-02-15T23:59:59Z)")
):
    """
    Retourne la liste des auteurs distincts ayant publié des tweets sur une période donnée.
    
    Args:
        start_date (datetime): Date de début (ISO 8601 avec timezone) - OBLIGATOIRE
        end_date (datetime): Date de fin (ISO 8601 avec timezone) - OBLIGATOIRE
        
    Returns:
        dict: {"usernames": ["@username1", "@username2", ...]}
    """
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT DISTINCT username
            FROM public.tweets
            WHERE created_at >= %s AND created_at <= %s
            ORDER BY username;
            """,
            (start_date, end_date)
        )

        usernames = [row[0] for row in cur.fetchall()]
        cur.close()

    return {"usernames": usernames}

@app.get("/api/twitter_conflicts/daily_summaries")
def get_country_summaries(
    country: str  # annotation manquante (= au lieu de :)
):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                SUMMARY_DATE::DATE,
                SUMMARY_TEXT
            FROM
                DAILY_SUMMARIES
            WHERE
                COUNTRY = %s
            ORDER BY
                SUMMARY_DATE DESC
            LIMIT 30
            """,
            (country,)
        )
        summaries = [
            {"date": row[0], "summary": row[1]}  
            for row in cur.fetchall()
        ]
        cur.close()
    return {"summaries": summaries}


@app.get("/api/twitter_conflicts/tweets.geojson")
def get_tweets(
    start_date: datetime = Query(..., description="Date de début (ISO 8601, ex: 2026-02-14T00:00:00Z)"),
    end_date: datetime = Query(..., description="Date de fin (ISO 8601, ex: 2026-02-15T23:59:59Z)"),
    q: Optional[str] = None,
    usernames: Optional[str] = None,
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
        usernames (str, optional): Liste d'auteurs séparés par virgules
        area (str, optional): Nom de la zone géographique
        
    Returns:
        Response: GeoJSON FeatureCollection
    """
    conditions = ["created_at >= %s AND created_at <= %s"]
    params = [start_date, end_date]

    if q:
        conditions.append("(text ILIKE %s OR username ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])

    if usernames:
        username_list = [a.strip() for a in usernames.split(',') if a.strip()]
        if username_list:
            placeholders = ','.join(['%s'] * len(username_list))
            conditions.append(f"username IN ({placeholders})")
            params.extend(username_list)

    if area:
        conditions.append("""wa.entity_name = %s""")
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
                            'id',               t.tweet_id,
                            'url',              t.tweet_url,
                            'username',         t.username,
                            'created_at',   t.created_at,
                            'text',             t.text,
                            'location_accuracy',         t.location_accuracy,
							'location_name', t.location_name,
							'latitude', st_y(t.geom),
							'longitude', st_x(t.geom),
                            'importance_score',       t.importance_score,
                            'conflict_typology',         t.conflict_typology,
                            'area_name',        wa.entity_name,
                            'images', COALESCE(
                                (
                                    SELECT JSON_AGG(ti.image_url ORDER BY ti.image_url)
                                    FROM public.tweet_images ti
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
            tension_level = result[2]
            evenements_json = result[3]
            cur.close()

            return {
                "country": country,
                "tension_score": int(tension_score),
                "tension_level": tension_level,
                "evenements": evenements_json
            }
            
        except Exception:
            cur.close()
            return None


@app.get("/api/twitter_conflicts/military_actions.geojson")
def get_military_actions(
    aggressor: Optional[str] = None,
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
):
    """
    Retourne les actions militaires en format GeoJSON (lignes aggressor → target).
    Args:
        start_date (datetime): Date de début - OBLIGATOIRE
        end_date (datetime): Date de fin - OBLIGATOIRE
        aggressor (str, optional): Nom du pays agresseur
    Returns:
        Response: GeoJSON FeatureCollection
    """
    conditions = []
    params = []

    if start_date and end_date:
        conditions.append("t.created_at >= %s AND t.created_at <= %s")
        params.extend([start_date, end_date])

    if aggressor:
        conditions.append("m.aggressor = %s")
        params.append(aggressor)

    conditions.append("ST_MAKELINE(m.aggressor_geom, m.target_geom) IS NOT NULL")
    where_clause = " AND ".join(conditions) if conditions else "1=1"

    query = f"""
        SELECT
            JSON_BUILD_OBJECT(
                'type', 'FeatureCollection',
                'features', COALESCE(JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(ST_MAKELINE(m.aggressor_geom, m.target_geom))::JSON,
                        'properties', JSON_BUILD_OBJECT(
                            'tweet_id',   m.tweet_id,
                            'created_at', t.created_at::DATE,
                            'aggressor',  m.aggressor,
                            'target',     m.target,
                            'action',     m.action
                        )
                    )
                ), '[]'::JSON)
            )
        FROM public.military_actions m
        LEFT JOIN public.tweets t ON t.tweet_id = m.tweet_id
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

    return Response(content=json.dumps(geojson_data, default=str), media_type="application/json")

@app.get("/api/twitter_conflicts/aggressor_range.geojson")
def get_aggressor_range(
    aggressor: str = Query(..., description="Nom du pays agresseur (ex: 'Israel')"),
):
    query = """
        SELECT
            JSON_BUILD_OBJECT(
                'type', 'FeatureCollection',
                'features', JSON_BUILD_ARRAY(
                    JSON_BUILD_OBJECT(
                        'type', 'Feature',
                        'geometry', ST_ASGEOJSON(
                            ST_INTERSECTION(
                                ST_MAKEENVELOPE(-179, -60, 179, 75, 4326),
                                ST_BUFFER(
                                    aggressor_geom,
                                    ST_DISTANCE(aggressor_geom, target_geom)
                                )
                            )
                        )::JSON,
                        'properties', JSON_BUILD_OBJECT(
                            'aggressor', aggressor
                        )
                    )
                )
            )
        FROM MILITARY_ACTIONS
        WHERE AGGRESSOR = %s
        ORDER BY ST_DISTANCE(aggressor_geom, target_geom) DESC
        LIMIT 1;
    """
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(query, [aggressor])
        geojson_data = cur.fetchone()[0] or {
            "type": "FeatureCollection",
            "features": []
        }
        cur.close()
    return Response(content=json.dumps(geojson_data, default=str), media_type="application/json")