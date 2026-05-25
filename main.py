"""
REST API FastAPI for accessing geolocated OSINT data
=====================================================

This API exposes OSINT tweet data stored in PostgreSQL/PostGIS
through several endpoints enabling map visualization and analysis.

Main endpoints:
- /tweets.geojson      : Geolocated tweets (GeoJSON format)
- /usernames           : List of active authors
- /important_tweets    : Strategic events (importance_score >= 4)
- /random_tweets       : Sample of non-geolocated tweets
- /disputed_areas.geojson : Conflict zones (polygons)

Configuration:
- Database: PostgreSQL with PostGIS exthreat
- CORS: Enabled for local development
- Environment variables: Loaded from .env file
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
from collections import defaultdict

load_dotenv()

# PostgreSQL/PostGIS database connection configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "twitter_conflicts"),
    "user": os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "disable"),
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 5,
    "keepalives_count": 5,
}

connection_pool = pool.ThreadedConnectionPool(
    minconn=2,
    maxconn=15,
    **DB_CONFIG
)

@contextmanager
def get_db():
    """
    Context manager that borrows a connection from the pool and returns it
    automatically at the end of the block, even if an exception is raised.

    Usage:
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

# CORS configuration to allow requests from the frontend
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


@app.get("/shipping_lanes.geojson")
def get_shipping_lanes():
    """
    Returns major and middle shipping lanes as a GeoJSON FeatureCollection.
    Geometries are simplified with a tolerance of 0.01 degrees for performance.
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

    return Response(content=json.dumps(geojson_data), media_type="application/geo+json")


@app.get("/world_areas.geojson")
def get_world_areas():
    """
    Returns major and middle shipping lanes as a GeoJSON FeatureCollection.
    Geometries are simplified with a tolerance of 0.01 degrees for performance.
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
                            JSON_BUILD_OBJECT('id', ID, 'name', ENTITY_NAME)
                        )
                    )
                )
            FROM
                WORLD_AREAS
            WHERE
                ENTITY_NAME IN (
                    SELECT
                        ENTITY_NAME
                    FROM
                        MILITARY_ACTIONS
                        NATURAL JOIN TWEETS
                        LEFT JOIN WORLD_AREAS WA ON ST_CONTAINS (WA.GEOM, TWEETS.GEOM)
                    WHERE
                        CREATED_AT >= NOW() - INTERVAL '14 days'
                        AND ENTITY_TYPE != 'marine region'
                    GROUP BY
                        ENTITY_NAME
                )
        """
        )

        geojson_data = cur.fetchone()[0]
        cur.close()

    return Response(content=json.dumps(geojson_data), media_type="application/geo+json")


@app.get("/chokepoints.geojson")
def get_checkpoints():
    """
    Returns all maritime chokepoints as a GeoJSON FeatureCollection.
    Each feature includes the chokepoint ID and port name.
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
                            ST_ASGEOJSON(GEOM)::JSON,
                            'properties',
                            JSON_BUILD_OBJECT(
                                'portname',
                                PORTNAME,
                                'status',
                                STATUS,
                                'confidence',
                                CONFIDENCE,
                                'reason',
                                REASON
                            )
                        )
                    )
                )
            FROM
                (
                    SELECT
                        CP.PORTNAME,
                        STATUS,
                        CONFIDENCE,
                        REASON,
                        GEOM
                    FROM
                        CHOKEPOINTS_STATE_HISTORY CS
                        LEFT JOIN CHOKEPOINTS CP ON CP.PORTNAME = CS.PORTNAME
                    WHERE
                        SNAPSHOT_DATE::DATE = CURRENT_DATE
                ) AS SUBQUERY;
        """
        )

        geojson_data = cur.fetchone()[0]
        cur.close()

    return Response(content=json.dumps(geojson_data))


@app.get("/usernames")
def get_usernames(
    start_date: datetime = Query(..., description="Start date (e.g. 2026-02-14T00:00:00Z)"),
    end_date: datetime = Query(..., description="End date (e.g. 2026-02-15T23:59:59Z)")
):
    """
    Returns the list of distinct authors who published tweets over a given time range.

    Args:
        start_date (datetime): Start date (with timezone) - REQUIRED
        end_date (datetime): End date (with timezone) - REQUIRED

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


@app.get("/last_update")
def get_last_update(
):
    """
    """
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CREATED_AT
            FROM
                TWEETS
            ORDER BY
                CREATED_AT DESC
            LIMIT
                1
            """
        )

        last_update =cur.fetchone()
        cur.close()

    return {"last_update": last_update[0]}


@app.get("/tweets.geojson")
def get_tweets(
    start_date: datetime = Query(..., description="Start date (e.g. 2026-02-14T00:00:00Z)"),
    end_date: datetime = Query(..., description="End date (e.g. 2026-02-15T23:59:59Z)"),
    q: Optional[str] = Query(None, description="Full-text search query (matches tweet text or username)"),
    usernames: Optional[str] = Query(None, description="Comma-separated list of authors to filter by"),
    area: Optional[str] = Query(None, description="Geographic area name to filter by"),
    aggressor: Optional[str] = Query(None, description="Filter by aggressor country name"),
    format: str = Query("geojson", description="Response format (default: geojson)"),
    sort: str = Query("date_desc", description="Sort order (default: date_desc)"),
    page: int = Query(1, description="Page number for pagination"),
    size: int = Query(50, description="Number of results per page")
):
    """
    Returns geolocated tweets as a GeoJSON FeatureCollection with advanced filtering.

    Args:
        start_date (datetime): Start date - REQUIRED
        end_date (datetime): End date - REQUIRED
        q (str, optional): Full-text search (matches tweet body or username)
        usernames (str, optional): Comma-separated list of authors
        area (str, optional): Geographic area name

    Returns:
        Response: GeoJSON FeatureCollection
    """
    conditions = ["T.created_at >= %s AND T.created_at <= %s"]
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

    if aggressor: 
        conditions.append("""ma.aggressor = %s""")
        params.append(aggressor)

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
                            'created_at',       t.created_at,
                            'text',             t.text,
                            'location_accuracy',         t.location_accuracy,
                            'nominatim_query',    t.nominatim_query,
                            'latitude',         ROUND(st_y(t.geom)::numeric, 3),
                            'longitude',        ROUND(st_x(t.geom)::numeric, 3),
                            'importance_score',          t.importance_score,
                            'conflict_typology',         t.conflict_typology,
                            'images', COALESCE(
                                (
                                    SELECT JSON_AGG(ti.image_url ORDER BY ti.image_url)
                                    FROM public.tweet_images ti
                                    WHERE ti.tweet_id = t.tweet_id
                                ),
                                '[]'::JSON
                            ),
                            'action',
					        MA.WEAPON_TYPE,
                            'aggressor',
                            aggressor,
                            'target',
                            target,
                            'weapon_type',
                            MA.weapon_type,
                            'entity_name',
                            wa.entity_name,
                            'verified',
                            verified,
                            'location_source',
                            location_source,
                            'label',
                            TOP.label
                        )
                    )
                )
            )
        FROM public.tweets t
        LEFT JOIN LATERAL (
            SELECT entity_name
            FROM public.world_areas
            WHERE ST_Contains(geom, t.geom)
            LIMIT 1
        ) wa ON TRUE
        LEFT JOIN LATERAL (
            SELECT weapon_type, aggressor, target
            FROM MILITARY_ACTIONS
            WHERE TWEET_ID = T.TWEET_ID
            LIMIT 1
        ) MA ON TRUE
        LEFT JOIN TOPICS TOP ON TOP.TOPIC_ID = T.FK_TOPIC
        WHERE {where_clause} AND IS_DUPLICATE = 'false' AND t.GEOM IS NOT NULL;
    """

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(query, params)

        geojson_data = cur.fetchone()[0] or {
            "type": "FeatureCollection",
            "features": []
        }
        cur.close()

    return Response(content=json.dumps(geojson_data), media_type="application/geo+json")


@app.get("/conflict_borders.geojson")
def get_conflict_borders():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
        WITH CONFLICTS AS (
            SELECT
                CASE WHEN MA.AGGRESSOR = 'TTP' THEN 'Afghanistan' ELSE MA.AGGRESSOR END AS AGGRESSOR,
                CASE WHEN MA.TARGET    = 'TTP' THEN 'Afghanistan' ELSE MA.TARGET    END AS TARGET
            FROM
                MILITARY_ACTIONS MA
                NATURAL JOIN TWEETS T
            WHERE
                CREATED_AT >= NOW() - INTERVAL '14 days'
                AND AGGRESSOR IS NOT NULL
                AND TARGET IS NOT NULL
            GROUP BY
                MA.AGGRESSOR,
                MA.TARGET
            HAVING
                COUNT(MA.AGGRESSOR) >= 3
                AND COUNT(MA.TARGET) >= 3
        ),
        CONFLICT_PAIRS AS (
            SELECT DISTINCT
                LEAST(C.AGGRESSOR, C.TARGET)    AS COUNTRY_A,
                GREATEST(C.AGGRESSOR, C.TARGET) AS COUNTRY_B
            FROM CONFLICTS C
            WHERE C.AGGRESSOR <> C.TARGET
        ),
        BORDERS AS (
            SELECT
                CP.COUNTRY_A,
                CP.COUNTRY_B,
                ST_Intersection(WA1.GEOM, WA2.GEOM) AS SHARED_BORDER
            FROM
                CONFLICT_PAIRS CP
                JOIN WORLD_AREAS WA1 ON WA1.ENTITY_NAME = CP.COUNTRY_A
                JOIN WORLD_AREAS WA2 ON WA2.ENTITY_NAME = CP.COUNTRY_B
            WHERE
                ST_Touches(WA1.GEOM, WA2.GEOM)
                OR ST_Intersects(WA1.GEOM, WA2.GEOM)
        )
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(
                json_build_object(
                    'type',       'Feature',
                    'geometry',   ST_AsGeoJSON(SHARED_BORDER)::json,
                    'properties', json_build_object(
                        'country_a', COUNTRY_A,
                        'country_b', COUNTRY_B
                    )
                )
            )
        ) AS geojson
        FROM BORDERS;
        """)

        geojson_data = cur.fetchone()[0]
        cur.close()

    return Response(content=json.dumps(geojson_data))


@app.get("/conflict_theaters.geojson")
def get_conflict_theaters():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
        WITH CONFLICTS AS (
            SELECT
                CASE WHEN MA.AGGRESSOR = 'TTP' THEN 'Afghanistan' ELSE MA.AGGRESSOR END AS AGGRESSOR,
                CASE WHEN MA.TARGET    = 'TTP' THEN 'Afghanistan' ELSE MA.TARGET    END AS TARGET
            FROM
                MILITARY_ACTIONS MA
                NATURAL JOIN TWEETS T
            WHERE
                CREATED_AT >= NOW() - INTERVAL '14 days'
                AND AGGRESSOR IS NOT NULL
                AND TARGET IS NOT NULL
            GROUP BY
                MA.AGGRESSOR,
                MA.TARGET
            HAVING
                COUNT(MA.AGGRESSOR) >= 3
                AND COUNT(MA.TARGET) >= 3
        ),
        CONFLICT_PAIRS AS (
            SELECT DISTINCT
                LEAST(C.AGGRESSOR, C.TARGET)    AS COUNTRY_A,
                GREATEST(C.AGGRESSOR, C.TARGET) AS COUNTRY_B
            FROM CONFLICTS C
            WHERE C.AGGRESSOR <> C.TARGET
        ),
        SHARED_BORDERS AS (
            SELECT
                CP.COUNTRY_A,
                CP.COUNTRY_B,
                ST_Intersection(WA1.GEOM, WA2.GEOM) AS BORDER_GEOM
            FROM
                CONFLICT_PAIRS CP
                JOIN WORLD_AREAS WA1 ON WA1.ENTITY_NAME = CP.COUNTRY_A
                JOIN WORLD_AREAS WA2 ON WA2.ENTITY_NAME = CP.COUNTRY_B
            WHERE
                ST_Touches(WA1.GEOM, WA2.GEOM)
                OR ST_Intersects(WA1.GEOM, WA2.GEOM)
        ),
        BORDER_REGIONS AS (
            SELECT DISTINCT
                WR.NAME,
                WR.COUNTRY_NAME,
                SB.COUNTRY_A,
                SB.COUNTRY_B,
                WR.GEOM
            FROM
                SHARED_BORDERS SB
                JOIN WORLD_REGIONS WR
                    ON ST_DWithin(WR.GEOM, SB.BORDER_GEOM, 0.5)
            WHERE
                WR.COUNTRY_NAME IN (SB.COUNTRY_A, SB.COUNTRY_B)
        ),
        MERGED_REGIONS AS (
            SELECT
                COUNTRY_A,
                COUNTRY_B,
                ST_Union(GEOM) AS GEOM
            FROM BORDER_REGIONS
            GROUP BY COUNTRY_A, COUNTRY_B
        )
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(
                json_build_object(
                    'type',       'Feature',
                    'geometry',   ST_AsGeoJSON(GEOM)::json,
                    'properties', json_build_object(
                        'country_a', COUNTRY_A,
                        'country_b', COUNTRY_B
                    )
                )
            )
        ) AS geojson
        FROM MERGED_REGIONS;    
        """)

        geojson_data = cur.fetchone()[0]
        cur.close()

    return Response(content=json.dumps(geojson_data))


@app.get("/important_tweets")
def get_important_tweets():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                CREATED_AT,
                SUMMARY_TEXT
            FROM
                TWEETS
            WHERE
                CREATED_AT >= NOW() - INTERVAL '72 hours'
                AND IMPORTANCE_SCORE >= 4
                AND CONFLICT_TYPOLOGY = 'POL'
            ORDER BY
	            CREATED_AT desc
            limit 15;
        """)
        rows = cur.fetchall()
        cur.close()

    important_tweets = {"important_tweets": [
        {
            "date": row[0],          
            "text": row[1],    

        }
        for row in rows
    ]}
    return important_tweets


@app.get("/topics")
def get_important_tweets():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
        SELECT
            COUNT(TWEET_ID),
            LABEL,
            COUNTRIES,
            ACTIVE,
            TOPIC_ID,
            TOPIC_SUMMARY,
            ST_X (TOPICS.GEOM),
            ST_Y (TOPICS.GEOM),
            MAX(TWEETS.CREATED_AT) AS LATEST_UPDATE
        FROM
            TWEETS
            LEFT JOIN TOPICS ON FK_TOPIC = TOPIC_ID
        WHERE
            TOPIC_ID IS NOT NULL
            AND IMPORTANCE_SCORE >= 4
        GROUP BY
            LABEL,
            COUNTRIES,
            ACTIVE,
            TOPIC_ID,
            TOPIC_SUMMARY,
            ST_X (TOPICS.GEOM),
            ST_Y (TOPICS.GEOM)
        ORDER BY
            MAX(TWEETS.CREATED_AT) DESC
        """)
        rows = cur.fetchall()
        cur.close()

    topics = {"topics": [
        {   
            "LABEL": row[1],          
            "COUNTRIES": row[2],    
            "ACTIVE": row[3], 
            "TOPIC_ID": row[4],
            "TOPIC_SUMMARY": row[5],
            "LNG": row[6],   
            "LAT": row[7],
            "LATEST_UPDATE": row[8],
        }
        for row in rows
    ]}
    return topics


@app.get("/topics/{topic_id}")
def get_topic_tweets(topic_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
        SELECT
            *
        FROM
            TOPIC_SUMMARIES
        WHERE FK_TOPIC = %s
        """, (topic_id,))
        rows = cur.fetchall()
        cur.close()
 
    events = {"tweets": [
        {
            "tweet_id":     row[0],
            "created_at":   row[1].isoformat(),
            "summary": row[2],
            "summary_title": row[3]
        }
        for row in rows
    ]}
    return events


@app.get("/conflict_areas.geojson")
def get_conflict_areas():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
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
                        JSON_BUILD_OBJECT('name', NAME, 'count', TWEET_COUNT)
                    )
                )
            )
        FROM
            (
                SELECT
                    NAME,
                    COUNT(NAME) AS TWEET_COUNT,
                    WR.GEOM
                FROM
                    TWEETS T
                    LEFT JOIN WORLD_REGIONS WR ON ST_INTERSECTS (WR.GEOM, T.GEOM)
                WHERE
                    CREATED_AT >= NOW() - INTERVAL '24 hours'
                    AND T.GEOM IS NOT NULL
                    AND IS_DUPLICATE = 'false'
                GROUP BY
                    NAME,
                    WR.GEOM
                HAVING
                    COUNT(NAME) >= 3
            ) SUB;   
        """)

        geojson_data = cur.fetchone()[0]
        cur.close()

    return Response(content=json.dumps(geojson_data))


@app.get("/military_lines.geojson")
def get_military_lines(
    country: str = Query(..., description="Nom du pays (AGGRESSOR)"),
    start_date: datetime = Query(...),
    end_date: datetime = Query(...)
):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                JSON_BUILD_OBJECT(
                    'type', 'FeatureCollection',
                    'features', JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'type', 'Feature',
                            'geometry', ST_ASGEOJSON(ST_MAKELINE(AGGRESSOR_GEOM, T.GEOM))::JSON,
                            'properties', JSON_BUILD_OBJECT('weapon_type', WEAPON_TYPE)
                        )
                    )
                )
            FROM
                MILITARY_ACTIONS MA
                NATURAL JOIN TWEETS T
            WHERE
                CREATED_AT >= %s
                AND CREATED_AT <= %s
                AND AGGRESSOR = %s
        """, (start_date, end_date, country))

        geojson_data = cur.fetchone()[0]
        cur.close()

    return Response(content=json.dumps(geojson_data), media_type="application/geo+json")