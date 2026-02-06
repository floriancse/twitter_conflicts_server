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

    # Requête SQL avec construction GeoJSON intégrée
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


@app.get("/api/twitter_conflicts/random_tweets")
def get_random_tweets(hours: int = 24):
    """
    Retourne un échantillon aléatoire de tweets NON géolocalisés.
    
    Args:
        hours (int): Période temporelle en heures (par défaut 24h)
        
    Returns:
        dict: {"tweets": [{id, body, author, date_published, url}, ...]}
        
    Critères de sélection :
    - Longueur de texte entre 50 et 200 caractères (tweets informatifs mais concis)
    - Pas de géométrie (GEOM IS NULL) - tweets non géolocalisables par le LLM
    - Limite : 5 tweets aléatoires
    
    Cas d'usage : Afficher du contenu contextuel sur l'interface sans surcharger la carte.
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

    # Formatage en JSON avec conversion des dates en ISO 8601
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
    Retourne les tweets d'importance stratégique élevée (score ≥ 4/5).
    
    Args:
        hours (int): Période temporelle en heures (par défaut 24h)
        
    Returns:
        dict: {"tweets": [{id, body, author, date_published, url, long, lat}, ...]}
        
    Critère de sélection :
    - Importance ≥ 4 (événements stratégiques/critiques selon l'analyse LLM)
    - Triés par date décroissante (plus récents en premier)
    
    Inclut les coordonnées GPS pour permettre le centrage de carte sur l'événement.
    Utilisé pour générer des alertes ou mettre en avant les développements majeurs.
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

    # Formatage avec extraction des coordonnées via ST_X/ST_Y (fonctions PostGIS)
    formatted_tweets = []
    for tweet in tweets:
        formatted_tweets.append({
            "id": tweet[0],
            "body": tweet[1],
            "author": tweet[2],
            "date_published": tweet[3].isoformat(),
            "url": tweet[4],
            "long": tweet[5],  # Longitude extraite avec ST_X
            "lat": tweet[6],   # Latitude extraite avec ST_Y
        })

    return {"tweets": formatted_tweets}