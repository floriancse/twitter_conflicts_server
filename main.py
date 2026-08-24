"""
REST API FastAPI for accessing geolocated OSINT data
=====================================================

This API exposes OSINT tweet data stored in PostgreSQL/PostGIS
through several endpoints enabling map visualization and analysis.

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


@app.get("/bootstrap")
def get_bootstrap():
    """
    Bundles all the "static" layers needed for the map's initial load into
    a single HTTP request:
    shipping_lanes, chokepoints, conflict_borders, conflict_theaters,
    conflict_areas, world_areas, topics_location, topics_areas,
    strait_closure, military_lines, tweets, topics, topic_summaries.

    A single connection is borrowed from the pool to run all the queries
    (instead of opening/closing one connection per endpoint on the
    frontend side).

    Returns:
        dict: {
            "shipping_lanes": <GeoJSON>,
            "chokepoints": <GeoJSON>,
            "conflict_borders": <GeoJSON>,
            "conflict_theaters": <GeoJSON>,
            "conflict_areas": <GeoJSON>,
            "world_areas": <GeoJSON>,
            "topics_location": <GeoJSON>,
            "topics_areas": <GeoJSON>,
            "strait_closure": [...],
            "military_lines": <GeoJSON>,  # fixed 30-day window
            "tweets": <GeoJSON>,          # fixed 30-day window
            "topics": [...],              # list of "important" topics (importance_score >= 4)
            "topic_summaries": {...},     # { topic_id: [ {tweet_id, created_at, summary, summary_title}, ... ] }, 15 max per topic
        }
    """
    with get_db() as conn:
        cur = conn.cursor()
        result = {}

        # --- shipping_lanes.geojson ---
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
        result["shipping_lanes"] = cur.fetchone()[0]

        # --- chokepoints.geojson ---
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
                            JSON_BUILD_OBJECT(
                                'portname',
                                PORTNAME,
                                'status',
                                STATUS,
                                'confidence',
                                CONFIDENCE,
                                'reason',
                                REASON,
                                'STATE_DURATION',
                                STATE_DURATION
                            )
                        )
                    )
                )
            FROM
                (
                    SELECT
                        CP.PORTNAME,
                        CS.STATUS,
                        CS.CONFIDENCE,
                        CS.REASON,
                        CP.GEOM,
                        CASE
                            WHEN CS.STATUS IN ('CLOSED', 'RESTRICTED') THEN CASE
                                WHEN MAX(CS2.SNAPSHOT_DATE::DATE) - COALESCE(
                                    MAX(CS2.SNAPSHOT_DATE::DATE) FILTER (
                                        WHERE
                                            CS2.STATUS = 'OPENED'
                                    ),
                                    MIN(CS2.SNAPSHOT_DATE::DATE)
                                ) = 0 THEN 1
                                ELSE MAX(CS2.SNAPSHOT_DATE::DATE) - COALESCE(
                                    MAX(CS2.SNAPSHOT_DATE::DATE) FILTER (
                                        WHERE
                                            CS2.STATUS = 'OPENED'
                                    ),
                                    MIN(CS2.SNAPSHOT_DATE::DATE)
                                )
                            END
                            WHEN CS.STATUS = 'OPENED' THEN MAX(CS2.SNAPSHOT_DATE::DATE) - COALESCE(
                                MAX(CS2.SNAPSHOT_DATE::DATE) FILTER (
                                    WHERE
                                        CS2.STATUS IN ('CLOSED', 'RESTRICTED')
                                ),
                                MIN(CS2.SNAPSHOT_DATE::DATE)
                            )
                        END AS STATE_DURATION
                    FROM
                        CHOKEPOINTS_STATE_HISTORY CS
                        LEFT JOIN CHOKEPOINTS CP ON CP.PORTNAME = CS.PORTNAME
                        LEFT JOIN CHOKEPOINTS_STATE_HISTORY CS2 ON CS2.PORTNAME = CS.PORTNAME
                    WHERE
                        CS.SNAPSHOT_DATE::DATE = (SELECT MAX(SNAPSHOT_DATE::DATE) FROM CHOKEPOINTS_STATE_HISTORY)
                        AND (
                            CS.STATUS IN ('CLOSED', 'RESTRICTED')
                            OR (
                                CS.STATUS = 'OPENED'
                                AND (
                                    SELECT
                                        MAX(CS3.SNAPSHOT_DATE::DATE)
                                    FROM
                                        CHOKEPOINTS_STATE_HISTORY CS3
                                    WHERE
                                        CS3.PORTNAME = CS.PORTNAME
                                        AND CS3.STATUS IN ('CLOSED', 'RESTRICTED')
                                ) >= CURRENT_DATE - 8
                            )
                        )
                    GROUP BY
                        CP.PORTNAME,
                        CS.STATUS,
                        CS.CONFIDENCE,
                        CS.REASON,
                        CP.GEOM
                )
            """
        )
        result["chokepoints"] = cur.fetchone()[0]

        # --- conflict_borders.geojson ---
        cur.execute(
            """
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
            """
        )
        result["conflict_borders"] = cur.fetchone()[0]

        # --- conflict_theaters.geojson ---
        cur.execute(
            """
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
            """
        )
        result["conflict_theaters"] = cur.fetchone()[0]

        # --- world_areas.geojson ---
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
                            ST_ASGEOJSON (ST_SIMPLIFY (GEOM, 0.001), 4)::JSON,
                            'properties',
                            JSON_BUILD_OBJECT('id', ID, 'name', ENTITY_NAME)
                        )
                    )
                )
            FROM
                WORLD_AREAS
            WHERE entity_type = 'country'
            """
        )
        result["world_areas"] = cur.fetchone()[0]

        # --- topics_location.geojson ---
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
                        JSON_BUILD_OBJECT('label', LABEL, 'countries', COUNTRIES, 'topic_summary', TOPIC_SUMMARY, 'topic_id', TOPIC_ID)
                    )
                )
            )
        FROM
            (
        SELECT
            TOPIC_ID,
            LABEL,
            COUNTRIES,
            TOPIC_SUMMARY,
            ST_CENTROID (ST_COLLECT (ST_BUFFER (T1.GEOM, 1))) AS GEOM
        FROM
            TWEETS T1
            LEFT JOIN TOPICS T2 ON T1.FK_TOPIC = T2.TOPIC_ID
        WHERE
            CONFLICT_TYPOLOGY = 'MIL'
            AND LABEL IS NOT NULL
        GROUP BY
            TOPIC_ID,
            LABEL
            ) SUB;
            """
        )
        result["topics_location"] = cur.fetchone()[0]

        # --- topics_areas.geojson ---
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
                        JSON_BUILD_OBJECT('topic_id', TOPIC_ID, 'label', label)
                        )
                )
            )
        FROM
            (
        SELECT
            TOPIC_ID,
            LABEL,
            ST_CONVEXHULL (ST_COLLECT (ST_BUFFER (T1.GEOM, 1))) AS GEOM
        FROM
            TWEETS T1
            LEFT JOIN TOPICS T2 ON T1.FK_TOPIC = T2.TOPIC_ID
        WHERE
            CONFLICT_TYPOLOGY = 'MIL'
            AND LABEL IS NOT NULL
            AND is_duplicate = 'false'
            AND (is_delayed = 'false' OR is_delayed IS NULL)
        GROUP BY
            TOPIC_ID,
            LABEL
                    ) SUB;
            """
        )
        result["topics_areas"] = cur.fetchone()[0]

        # --- last_update ---
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
        result["last_update"] = cur.fetchone()[0]

        # --- strait_closure ---
        cur.execute(
            """
            WITH
                CLOSED_DATES AS (
                    SELECT DISTINCT
                        SNAPSHOT_DATE::DATE AS SNAP_DATE,
                        PORTNAME
                    FROM
                        CHOKEPOINTS_STATE_HISTORY
                    WHERE
                        STATUS IN ('CLOSED', 'RESTRICTED')
                        AND SNAPSHOT_DATE >= NOW() - INTERVAL '30 days'
                ),
                ISLANDS AS (
                    SELECT
                        PORTNAME,
                        SNAP_DATE,
                        SNAP_DATE - (
                            ROW_NUMBER() OVER (
                                PARTITION BY
                                    PORTNAME
                                ORDER BY
                                    SNAP_DATE
                            )
                        )::INT * INTERVAL '1 day' AS GRP
                    FROM
                        CLOSED_DATES
                ),
                ISLAND_RANGES AS (
                    SELECT
                        PORTNAME,
                        MIN(SNAP_DATE) AS CLOSURE_START,
                        MAX(SNAP_DATE) AS CLOSURE_END
                    FROM
                        ISLANDS
                    GROUP BY
                        PORTNAME,
                        GRP
                )
            SELECT DISTINCT
                ON (PORTNAME) PORTNAME,
                CLOSURE_START
            FROM
                ISLAND_RANGES
            ORDER BY
                PORTNAME,
                CLOSURE_END DESC;
            """
        )
        result["strait_closure"] = cur.fetchall()

        # --- military_lines ---
        cur.execute(
                    """
                    SELECT
                        JSON_BUILD_OBJECT(
                            'type', 'FeatureCollection',
                            'features', JSON_AGG(
                                JSON_BUILD_OBJECT(
                                    'type', 'Feature',
                                    'geometry', ST_ASGEOJSON(ST_MAKELINE (WC.GEOM, T.GEOM))::JSON,
                                    'properties', JSON_BUILD_OBJECT(
                                        'created_at', T.CREATED_AT,
                                        'label', TOP.LABEL,
                                        'weapon_type',weapon_type,
                                        'objective_type',objective_type,
                                        'text', T.TEXT
                                    )
                                )
                            )
                        )
                    FROM
                        MILITARY_ACTIONS MA
                        NATURAL JOIN TWEETS T
                        LEFT JOIN TOPICS TOP ON TOP.TOPIC_ID = T.FK_TOPIC
                        LEFT JOIN WORLD_AREAS WA ON wa.entity_name = aggressor
                        LEFT JOIN WORLD_CAPITALS WC ON ST_CONTAINS (WA.GEOM, WC.GEOM)
                    WHERE
                        t.created_at >= NOW() - INTERVAL '30 days'
                        AND t.is_duplicate = 'false'
                        AND t.geom IS NOT NULL
                        AND (t.is_delayed = 'false' OR t.is_delayed IS NULL)
                        AND NOT (
                            T.CONFLICT_TYPOLOGY = 'MIL'
                            AND T.NOMINATIM_QUERY NOT LIKE '%,%'
                        )
                    """
                )

        result["military_lines"] = cur.fetchone()[0]

        cur.execute(
            """
            WITH
                FILTERED_TWEETS AS (
                    SELECT
                        T.TWEET_ID,
                        T.TWEET_URL,
                        T.USERNAME,
                        T.CREATED_AT,
                        T.TEXT,
                        T.LOCATION_ACCURACY,
                        T.NOMINATIM_QUERY,
                        T.GEOM,
                        T.IMPORTANCE_SCORE,
                        T.CONFLICT_TYPOLOGY,
                        T.VERIFIED,
                        T.LOCATION_SOURCE,
                        T.IS_DUPLICATE,
                        T.FK_TOPIC
                    FROM
                        TWEETS T
                    WHERE
                        T.CREATED_AT >= NOW() - INTERVAL '30 days'
                        AND T.IS_DUPLICATE = 'false'
                        AND T.GEOM IS NOT NULL
                        AND (
                            T.IS_DELAYED = 'false'
                            OR T.IS_DELAYED IS NULL
                        )
                        AND NOT (
                            T.CONFLICT_TYPOLOGY = 'MIL'
                            AND T.NOMINATIM_QUERY NOT LIKE '%,%'
                        )
                )
            SELECT
                JSON_BUILD_OBJECT(
                    'type', 'FeatureCollection',
                    'features', JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'type', 'Feature',
                            'geometry', ST_AsGeoJSON(ft.geom)::JSON,
                            'properties', JSON_BUILD_OBJECT(
                                'id', ft.tweet_id,
                                'url', ft.tweet_url,
                                'username', ft.username,
                                'created_at', ft.created_at,
                                'text', ft.text,
                                'latitude', ROUND(st_y(ft.geom)::numeric, 3),
                                'longitude', ROUND(st_x(ft.geom)::numeric, 3),
                                'importance_score', ft.importance_score,
                                'conflict_typology', ft.conflict_typology,
                                'verified', ft.verified,
                                'location_source', ft.location_source,
                                'label', top.label,
                                'objective_type', ma.objective_type,
                                'weapon_type', ma.weapon_type,
                                'aggressor', ma.aggressor,
                                'target', ma.target,
                                'nominatim_query', ft.nominatim_query,
                                'images', COALESCE(
                                    (SELECT JSON_AGG(ti.image_url ORDER BY ti.image_url)
                                    FROM tweet_images ti
                                    WHERE ti.tweet_id = ft.tweet_id),
                                    '[]'::JSON
                                )
                            )
                        )
                    )
                )
            FROM filtered_tweets ft
            LEFT JOIN topics top ON top.topic_id = ft.fk_topic
            LEFT JOIN military_actions ma ON ma.tweet_id = ft.tweet_id
            """
        )
        result["tweets"] = cur.fetchone()[0]

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
            AND IS_DUPLICATE = 'false'
            AND (
                IS_DELAYED IS NULL
                OR IS_DELAYED = 'false'
            ) 
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
        topics_rows = cur.fetchall()
        result["topics"] = [
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
            for row in topics_rows
        ]

        cur.execute(
            """
            SELECT
                CREATED_AT,
                SUMMARY,
                SUMMARY_TITLE,
                FK_TOPIC
            FROM
                (
                    SELECT
                        CREATED_AT,
                        SUMMARY,
                        SUMMARY_TITLE,
                        FK_TOPIC,
                        ROW_NUMBER() OVER (
                            PARTITION BY
                                FK_TOPIC
                            ORDER BY
                                CREATED_AT DESC
                        ) AS RN
                    FROM
                        TOPIC_SUMMARIES
                ) SUB
            WHERE
                RN <= 15
            ORDER BY
                FK_TOPIC,
                CREATED_AT DESC
            """
        )
        summary_rows = cur.fetchall()
        topic_summaries = {}
        for row in summary_rows:
            created_at, summary, summary_title, topic_id = row
            topic_summaries.setdefault(topic_id, []).append({
                "created_at": created_at.isoformat(),
                "summary": summary,
                "summary_title": summary_title,
            })
        result["topic_summaries"] = topic_summaries

        cur.close()

    return result


@app.get("/graph_events")
def get_graph_events(
    weapon_type: Optional[List[str]] = Query(None, description="Filter by weapon(s) used. Repeat the param for multiple values, e.g. ?weapon_type=A&weapon_type=B"),
    objective_type: Optional[List[str]] = Query(None, description="Filter by objective(s) targeted. Repeat the param for multiple values, e.g. ?objective_type=A&objective_type=B"),
    label: Optional[str] = Query(None, description="Filter by topic label, e.g. ?label=Conflicts in Sahel"),
    search: Optional[str] = Query(None, description="Free-text filter on tweet content, e.g. ?search=Wildberries"),
):
    conditions = ["T.GEOM IS NOT NULL", "T.IS_DUPLICATE = 'false'", "T.FK_TOPIC IS NOT NULL"]
    params = []

    if weapon_type:
        placeholders = ','.join(['%s'] * len(weapon_type))
        conditions.append(f"MA.weapon_type IN ({placeholders})")
        params.extend(weapon_type)

    if objective_type:
        placeholders = ','.join(['%s'] * len(objective_type))
        conditions.append(f"MA.objective_type IN ({placeholders})")
        params.extend(objective_type)

    if label:
        conditions.append("TOPICS.LABEL = %s")
        params.append(label)

    if search:
        conditions.append("T.TEXT ILIKE %s")
        params.append(f"%{search}%")

    where_clause = " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            WITH MAX_DATE AS (
                SELECT MAX(CREATED_AT) AS MAX_CREATED_AT FROM TWEETS
            ),
            ANCHOR AS (
                SELECT
                    DATE_TRUNC('day', MD.MAX_CREATED_AT) +
                    INTERVAL '12 hours' * FLOOR(EXTRACT(HOUR FROM MD.MAX_CREATED_AT) / 12) AS PERIOD_START
                FROM MAX_DATE MD
            ),
            FILTERED_TWEETS AS (
                SELECT
                    T.CREATED_AT
                FROM
                    TWEETS T
                    LEFT JOIN MILITARY_ACTIONS MA ON T.TWEET_ID = MA.TWEET_ID
                    LEFT JOIN TOPICS ON T.FK_TOPIC = TOPICS.TOPIC_ID
                WHERE
                    T.CREATED_AT >= (SELECT PERIOD_START FROM ANCHOR) - INTERVAL '30 days'
                    AND {where_clause} 
                    AND TOPIC_ID IS NOT NULL
                    AND (
                        IS_DELAYED IS NULL
                        OR IS_DELAYED = 'false'
                    ) 
                    AND NOT (
                        T.CONFLICT_TYPOLOGY = 'MIL'
                        AND T.NOMINATIM_QUERY NOT LIKE '%%,%%'
                    )
            ),
            FULL_SERIES AS (
                SELECT
                    D.DAY AS DATE,
                    COUNT(FT.CREATED_AT) AS EVENTS
                FROM
                    GENERATE_SERIES(
                        (SELECT PERIOD_START FROM ANCHOR) - INTERVAL '30 days',
                        (SELECT PERIOD_START FROM ANCHOR) + INTERVAL '12 hours',
                        INTERVAL '12 hours'
                    ) AS D (DAY)
                    LEFT JOIN FILTERED_TWEETS FT ON FT.CREATED_AT >= D.DAY
                    AND FT.CREATED_AT < D.DAY + INTERVAL '12 hours'
                GROUP BY
                    D.DAY
            ),
            LAST_NON_EMPTY AS (
                SELECT MAX(DATE) AS LAST_DAY FROM FULL_SERIES WHERE EVENTS > 0
            )
            SELECT
                FS.DATE,
                FS.EVENTS
            FROM
                FULL_SERIES FS
                CROSS JOIN LAST_NON_EMPTY LN
            WHERE
                LN.LAST_DAY IS NOT NULL
                AND FS.DATE <= LN.LAST_DAY
            ORDER BY
                FS.DATE ASC
            """, params
        )
        graph_events = cur.fetchall()
        cur.close()

    return {
        "events": [
            {"date": date.isoformat(), "count": events}
            for date, events in graph_events
        ]
    }