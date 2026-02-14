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
- Connection Pooling : Pool de connexions pour performances optimales
"""

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from dotenv import load_dotenv
from psycopg2 import pool
import os
import json
from datetime import datetime, timedelta
from typing import Optional

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

# Pool de connexions (créé au démarrage de l'application)
connection_pool = None

app = FastAPI()

# Middleware GZIP pour compression automatique
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


@app.on_event("startup")
def startup_event():
    """
    Initialise le pool de connexions au démarrage de l'application.
    Crée 2 connexions minimum et permet jusqu'à 10 connexions simultanées.
    """
    global connection_pool
    try:
        connection_pool = pool.SimpleConnectionPool(
            minconn=2,      # Minimum 2 connexions toujours ouvertes
            maxconn=10,     # Maximum 10 connexions simultanées
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            dbname=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
        )
        print("✅ Database connection pool created successfully")
    except Exception as e:
        print(f"❌ Error creating connection pool: {e}")
        raise


@app.on_event("shutdown")
def shutdown_event():
    """
    Ferme proprement toutes les connexions du pool à l'arrêt de l'application.
    """
    global connection_pool
    if connection_pool:
        connection_pool.closeall()
        print("🔌 Database connection pool closed")


def get_db_connection():
    """
    Récupère une connexion depuis le pool.
    
    Returns:
        psycopg2.connection: Connexion active depuis le pool
        
    Note:
        Utiliser toujours dans un try/finally avec release_db_connection()
    """
    if connection_pool is None:
        raise Exception("Connection pool not initialized")
    return connection_pool.getconn()


def release_db_connection(conn):
    """
    Libère une connexion vers le pool pour réutilisation.
    
    Args:
        conn: Connexion à libérer
        
    Note:
        TOUJOURS appeler cette fonction dans un bloc finally
    """
    if connection_pool and conn:
        connection_pool.putconn(conn)


@app.get("/api/health")
def health_check():
    """
    Endpoint de santé pour vérifier que l'API et la base de données répondent.
    
    Returns:
        dict: Status et timestamp
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        
        return {
            "status": "ok",
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "error",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
    finally:
        if conn:
            release_db_connection(conn)


@app.get("/api/twitter_conflicts/disputed_area.geojson")
def get_disputed_area():
    """
    Retourne les zones de conflit/contestées en format GeoJSON.
    
    Utilise les fonctions PostGIS pour convertir les géométries PostgreSQL
    en GeoJSON standard compatible avec les bibliothèques cartographiques (Leaflet, Mapbox).
    
    Returns:
        Response: GeoJSON FeatureCollection contenant les polygones des zones disputées
    """
    conn = None
    try:
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
                        'geometry', ST_AsGeoJSON(ST_Simplify(geom, 0.01), 4)::json,
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
        
    finally:
        if conn:
            release_db_connection(conn)


@app.get("/api/twitter_conflicts/world_areas.geojson")
def get_world_areas():
    """
    Retourne les frontières des pays du monde en format GeoJSON optimisé.
    
    Optimisations appliquées:
    - Simplification des géométries (ST_Simplify)
    - Réduction de la précision à 4 décimales
    - Compression JSON compacte
    - GZIP automatique via middleware
    
    Returns:
        Response: GeoJSON FeatureCollection avec cache headers
    """
    conn = None
    try:
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

        # JSON ultra-compact : pas d'espaces, pas d'indentation
        compact_geojson = json.dumps(
            geojson_data,
            separators=(',', ':'),   # enlève tous les espaces inutiles
            ensure_ascii=False       # garde les accents français
        )

        return Response(
            content=compact_geojson,
            media_type="application/json",
            headers={
                "Cache-Control": "public, max-age=86400",  # Cache 24h
                "Vary": "Accept-Encoding"
            }
        )
        
    finally:
        if conn:
            release_db_connection(conn)


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
    conn = None
    try:
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

        return {"authors": authors}
        
    finally:
        if conn:
            release_db_connection(conn)


@app.get("/api/twitter_conflicts/tweets.geojson")
def get_tweets(
    hours: int = 24,
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
        hours (int): Période temporelle en heures (par défaut 24h)
        q (str, optional): Recherche textuelle (ILIKE sur body et author)
        authors (str, optional): Liste d'auteurs séparés par virgules (ex: "@user1,@user2")
        area (str, optional): Filtre par pays
        
    Returns:
        Response: GeoJSON FeatureCollection avec les tweets et leurs métadonnées
        
    Exemple d'utilisation :
        /api/twitter_conflicts/tweets.geojson?hours=48&q=missile&authors=@GeoConfirmed
    """
    conn = None
    try:
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

        # Filtre par pays
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

        return Response(content=json.dumps(geojson_data), media_type="application/json")
        
    finally:
        if conn:
            release_db_connection(conn)


@app.get("/api/twitter_conflicts/area_stats")
def get_area_stats(
    area_name: str,
    hours: int = 24
):
    """
    Retourne les statistiques d'événements par pays agrégées par période.
    
    Args:
        area_name (str): Nom du pays (ex: "Ukraine", "Russia")
        hours (int): Période en heures (24, 168=7j, 720=30j)
        
    Returns:
        dict: Données agrégées par intervalle de temps avec comptages
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Déterminer l'intervalle d'agrégation selon la période
        if hours <= 24:
            # 1 jour : agrégation toutes les 2 heures
            interval_hours = 2
            interval_sql = "2 hours"
        elif hours <= 168:  # 7 jours
            # 7 jours : agrégation toutes les 12 heures
            interval_hours = 12
            interval_sql = "12 hours"
        else:  # 30 jours
            # 30 jours : agrégation par jour
            interval_hours = 24
            interval_sql = "1 day"
        
        query = """
            WITH time_buckets AS (
                SELECT 
                    DATE_TRUNC('hour', date_published) + 
                    INTERVAL '%s' * FLOOR(EXTRACT(EPOCH FROM (date_published - DATE_TRUNC('hour', date_published))) / (EXTRACT(EPOCH FROM INTERVAL '%s'))) AS time_bucket,
                    typology
                FROM public.tweets t
                LEFT JOIN public.world_areas wa ON ST_Contains(wa.geom, t.geom)
                WHERE 
                    wa."NAME_FR" = %%s
                    AND date_published >= NOW() - INTERVAL '%%s hours'
            )
            SELECT 
                time_bucket,
                COUNT(*) as total
            FROM time_buckets
            GROUP BY time_bucket
            ORDER BY time_bucket ASC;
        """ % (interval_sql, interval_sql)
        
        cur.execute(query, (area_name, hours))
        
        results = cur.fetchall()
        
        data = []
        for row in results:
            data.append({
                "timestamp": row[0].isoformat() if row[0] else None,
                "total": row[1],
            })
        
        cur.close()
        
        return {
            "area": area_name,
            "period_hours": hours,
            "interval_hours": interval_hours,
            "data": data
        }
        
    finally:
        if conn:
            release_db_connection(conn)


@app.get("/api/twitter_conflicts/area_info")
def get_area_info(
    area_name: str,
    hours: int = 24
):
    """
    Retourne les informations générales d'un pays pour une période donnée.
    
    Args:
        area_name (str): Nom du pays
        hours (int): Période en heures
        
    Returns:
        dict: Statistiques générales du pays
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT author) as unique_authors,
                MAX(date_published) as last_event_date
            FROM public.tweets t
            LEFT JOIN public.world_areas wa ON ST_Contains(wa.geom, t.geom)
            WHERE 
                wa."NAME_FR" = %s
                AND date_published >= NOW() - INTERVAL '%s hours';
        """
        
        cur.execute(query, (area_name, hours))
        
        result = cur.fetchone()
        cur.close()
        
        return {
            "area": area_name,
            "period_hours": hours,
            "total_events":   result[0] if result else 0,
            "unique_authors": result[1] if result else 0,
            "last_event_date": result[2].isoformat() if result and result[2] else None
        }
        
    finally:
        if conn:
            release_db_connection(conn)