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
import os
import json
import geojson
from datetime import datetime, timedelta
from typing import Optional, List
import gzip
from fastapi.middleware.gzip import GZipMiddleware

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
app.add_middleware(GZipMiddleware, minimum_size=1000)

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
    conn.close()

    return Response(content=json.dumps(geojson_data), media_type="application/json")


@app.get("/api/twitter_conflicts/world_areas.geojson")
def get_world_areas():
    conn = get_db_connection()
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
    conn.close()

    # JSON ultra-compact : pas d'espaces, pas d'indentation
    compact_geojson = json.dumps(
        geojson_data,
        separators=(',', ':'),   # enlève tous les espaces inutiles
        ensure_ascii=False       # garde les accents français
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
        
    Utilisé pour alimenter les filtres de recherche dans l'interface utilisateur.
    
    Exemples d'utilisation :
        # Auteurs des 30 derniers jours
        /api/twitter_conflicts/authors?start_date=2026-01-15T00:00:00Z&end_date=2026-02-15T23:59:59Z
        
        # Auteurs d'aujourd'hui
        /api/twitter_conflicts/authors?start_date=2026-02-15T00:00:00Z&end_date=2026-02-15T23:59:59Z
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Requête avec plage temporelle explicite
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
    conn.close()

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
        start_date (datetime): Date de début (ISO 8601 avec timezone) - OBLIGATOIRE
        end_date (datetime): Date de fin (ISO 8601 avec timezone) - OBLIGATOIRE
        q (str, optional): Recherche textuelle (ILIKE sur body et author)
        authors (str, optional): Liste d'auteurs séparés par virgules (ex: "@user1,@user2")
        area (str, optional): Nom de la zone géographique (world_areas.NAME_FR)
        
    Returns:
        Response: GeoJSON FeatureCollection avec les tweets et leurs métadonnées
        
    Exemples d'utilisation :
        # Plage temporelle (7 jours)
        /api/twitter_conflicts/tweets.geojson?start_date=2026-02-08T00:00:00Z&end_date=2026-02-15T23:59:59Z
        
        # Avec filtres combinés
        /api/twitter_conflicts/tweets.geojson?start_date=2026-02-14T00:00:00Z&end_date=2026-02-15T23:59:59Z&q=missile&authors=@GeoConfirmed
        
        # Filtrer par zone
        /api/twitter_conflicts/tweets.geojson?start_date=2026-02-01T00:00:00Z&end_date=2026-02-15T23:59:59Z&area=Ukraine
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Construction dynamique de la clause WHERE
    conditions = ["date_published >= %s AND date_published <= %s"]
    params = [start_date, end_date]

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

    # Filtre par zone géographique
    if area:
        conditions.append("""wa."NAME_FR" = %s""")
        params.append(area)
        
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
                            'area_name',        wa."NAME_FR",
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
        LEFT JOIN public.world_areas wa
            ON ST_Contains(wa.geom, t.geom)
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

@app.get("/api/twitter_conflicts/tension_index")
def get_tweets(
    area: Optional[str] = None,         
):
    conn = get_db_connection()
    cur = conn.cursor()

    # Requête avec plage temporelle explicite
    cur.execute(
        """
        SELECT
            *
        FROM
            TENSION_INDEX_MV
        WHERE
            COUNTRY = %s
        """,
        (area, )
    )

    result = cur.fetchone()
    country = result[0]
    tension_score = result[1]
    niveau_tension = result[2]
    evenements_json = json.loads(result[3])

    cur.close()
    conn.close()

    return {"tension_score": tension_score}