"""
REST API FastAPI for accessing geolocated OSINT data
=====================================================

This API exposes OSINT tweet data stored in PostgreSQL/PostGIS
through several endpoints enabling map visualization and analysis.

Main endpoints:
- /api/twitter_conflicts/tweets.geojson      : Geolocated tweets (GeoJSON format)
- /api/twitter_conflicts/usernames           : List of active authors
- /api/twitter_conflicts/important_tweets    : Strategic events (importance_score >= 4)
- /api/twitter_conflicts/random_tweets       : Sample of non-geolocated tweets
- /api/twitter_conflicts/disputed_areas.geojson : Conflict zones (polygons)

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

load_dotenv()

# PostgreSQL/PostGIS database connection configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "twitter_conflicts"),
    "user": os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "disable"),
    # Keepalives to prevent network disconnections on OVH
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 5,
    "keepalives_count": 5,
}

# Connection pool: min 2, max 15 simultaneous connections
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
        media_type="application/geo+json"
    )


@app.get("/api/twitter_conflicts/current_frontline.geojson")
def get_current_frontline():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT JSON_BUILD_OBJECT(
                'type', 'FeatureCollection',
                'features', JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'type', 'Feature',
                        'geometry', ST_ASGEOJSON(geom.geom, 4)::json,
                        'properties', JSON_BUILD_OBJECT(
                            'aggressor', AGGRESSOR,
                            'target', TARGET
                        )
                    )
                )
            ) AS result
            FROM (
                SELECT
                    AGGRESSOR,
                    TARGET,
                    ST_LineMerge(
                        ST_CollectionExtract(
                            ST_INTERSECTION(A.GEOM, B.GEOM),
                        2)
                    ) AS INTERSECTION_GEOM
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
            LATERAL ST_DUMP(INTERSECTION_GEOM) AS geom
            WHERE ST_GeometryType(geom.geom) = 'ST_LineString'
              AND NOT ST_IsEmpty(geom.geom);
            """
        )
        geojson_data = cur.fetchone()[0]
        cur.close()

    return Response(content=json.dumps(geojson_data), media_type="application/geo+json")


@app.get("/api/twitter_conflicts/shipping_lanes.geojson")
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


@app.get("/api/twitter_conflicts/chokepoints.geojson")
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


@app.get("/api/twitter_conflicts/daily_summaries")
def get_country_summaries(
    country: str
):
    """
    Returns the last 30 daily summaries for a given country, ordered by most recent first.

    Args:
        country (str): Country name - REQUIRED

    Returns:
        dict: {"summaries": [{"date": str, "summary": str}, ...]}
    """
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
    start_date: datetime = Query(..., description="Start date (e.g. 2026-02-14T00:00:00Z)"),
    end_date: datetime = Query(..., description="End date (e.g. 2026-02-15T23:59:59Z)"),
    q: Optional[str] = Query(None, description="Full-text search query (matches tweet text or username)"),
    usernames: Optional[str] = Query(None, description="Comma-separated list of authors to filter by"),
    area: Optional[str] = Query(None, description="Geographic area name to filter by"),
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
                            'created_at',       t.created_at,
                            'text',             t.text,
                            'location_accuracy',         t.location_accuracy,
                            'location_name',    t.location_name,
                            'latitude',         st_y(t.geom),
                            'longitude',        st_x(t.geom),
                            'importance_score',          t.importance_score,
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

    return Response(content=json.dumps(geojson_data), media_type="application/geo+json")

@app.get("/api/twitter_conflicts/threat_index")
def get_threat_index(
    area: Optional[str] = Query(None, description="Geographic area name (e.g. 'Iran')")
):
    """
    Returns the most recent threat index for a given country.

    Args:
        area (str, optional): Country name to query

    Returns:
        dict: country, threat_score, threat_level, event_count, attacks_launched, attacks_received, raw_score, max_severity, snapshot_at
    """
    if not area:
        raise HTTPException(status_code=400, detail="Parameter 'area' is required")

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                snapshot_at,
                country,
                threat_score,
                event_count,
                attacks_launched,
                attacks_received,
                threat_level,
                raw_score,
                max_severity
            FROM country_threat_history
            WHERE country = %s
            ORDER BY snapshot_at DESC
            LIMIT 1
            """,
            (area,)
        )
        row = cur.fetchone()
        cur.close()

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No threat data found for country: {area}"
        )

    return {
        "snapshot_at":       row[0].isoformat(),
        "country":           row[1],
        "threat_score":     int(row[2]),
        "event_count":       row[3],
        "attacks_launched":  row[4],
        "attacks_received":  row[5],
        "threat_level":      row[6],
        "raw_score":         float(row[7]),
        "max_severity":      row[8],
    }

@app.get("/api/twitter_conflicts/military_actions.geojson")
def get_military_actions(
    aggressor: Optional[str] = Query(None, description="Aggressor country name (e.g. 'Russia')"),
    start_date: datetime = Query(..., description="Start date (e.g. 2026-02-14T00:00:00Z)"),
    end_date: datetime = Query(..., description="End date (e.g. 2026-02-15T23:59:59Z)"),
):
    """
    Returns military actions as a GeoJSON FeatureCollection (lines from aggressor to target).

    Args:
        start_date (datetime): Start date - REQUIRED
        end_date (datetime): End date - REQUIRED
        aggressor (str, optional): Aggressor country name

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

    return Response(content=json.dumps(geojson_data, default=str), media_type="application/geo+json")


@app.get("/api/twitter_conflicts/aggressor_range.geojson")
def get_aggressor_range(
    aggressor: str = Query(..., description="Aggressor country name (e.g. 'Israel')"),
):
    """
    Returns the estimated operational range of an aggressor as a convex hull polygon,
    built from its known target locations buffered by 100 km, plus the aggressor's capital buffered by 10 km.

    Args:
        aggressor (str): Aggressor country name - REQUIRED

    Returns:
        Response: GeoJSON FeatureCollection
    """
    query = """
        SELECT
    JSON_BUILD_OBJECT(
        'type', 'FeatureCollection',
        'features', JSON_AGG(
            JSON_BUILD_OBJECT(
                'type', 'Feature',
                'geometry', ST_ASGEOJSON(hull)::JSON,
                'properties', JSON_BUILD_OBJECT(
                    'entity_name', entity_name
                )
            )
        )
    )
FROM (
    SELECT
        A.ENTITY_NAME as entity_name,
        ST_CONVEXHULL(
            ST_COLLECT(
                ARRAY_AGG(
                    ST_Transform(
                        ST_Buffer(
                            ST_Transform(M.TARGET_GEOM, 3857),
                            100000
                        ),
                    4326)
                )
                || ARRAY[
                    ST_Transform(
                        ST_Buffer(
                            ST_Transform(C.GEOM, 3857),
                            10000
                        ),
                    4326)
                   ]
            )
        ) as hull
    FROM MILITARY_ACTIONS M
    LEFT JOIN WORLD_AREAS A ON ST_INTERSECTS(M.AGGRESSOR_GEOM, A.GEOM)
    LEFT JOIN WORLD_CAPITALS C ON ST_INTERSECTS(A.GEOM, C.GEOM)
    WHERE A.ENTITY_NAME = %s
      AND M.TARGET_GEOM IS NOT NULL
    GROUP BY A.ENTITY_NAME, C.GEOM
    HAVING COUNT(M.TARGET_GEOM) > 0
) sub
    """
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(query, [aggressor])
        result = cur.fetchone()[0]
        cur.close()

    if isinstance(result, str):
        geojson_data = result
    else:
        geojson_data = json.dumps(result, default=str)

    return Response(content=geojson_data, media_type="application/geo+json")


@app.get("/api/twitter_conflicts/country_threat_history")
def get_country_threat_history(
    country: str = Query(..., description="Country name (e.g. 'Iran')")
):
    """
    Returns the historical threat scores for a given country, ordered chronologically.

    Args:
        country (str): Country name - REQUIRED

    Returns:
        dict: {"country": str, "history": [{"snapshot_at": str, "threat_score": int, "threat_level": str, "event_count": int, "attacks_launched": int, "attacks_received": int}, ...]}
    """
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                SNAPSHOT_AT,
                threat_SCORE,
                THREAT_LEVEL,
                EVENT_COUNT,
                ATTACKS_LAUNCHED,
                ATTACKS_RECEIVED
            FROM
                country_threat_history
            WHERE
                COUNTRY = %s
            ORDER BY
                SNAPSHOT_AT ASC
            """,
            (country,)
        )
        rows = cur.fetchall()
        cur.close()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No threat history found for country: {country}"
        )

    history = [
        {
            "snapshot_at":       row[0].isoformat(),
            "threat_score":     row[1],
            "threat_level":      row[2],
            "event_count":       row[3],
            "attacks_launched":  row[4],
            "attacks_received":  row[5],
        }
        for row in rows
    ]

    return {"country": country, "history": history}