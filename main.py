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


@app.get("/bootstrap")
def get_bootstrap():
    """
    Regroupe en une seule requête HTTP les couches "statiques" nécessaires
    au chargement initial de la carte :
    shipping_lanes, chokepoints, conflict_borders, conflict_theaters,
    conflict_areas, world_areas, topics_location, topics_areas.

    Une seule connexion est empruntée au pool pour exécuter les 8 requêtes
    (au lieu d'ouvrir/fermer une connexion par endpoint côté front).

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

        # --- conflict_areas.geojson ---
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
            """
        )
        result["conflict_areas"] = cur.fetchone()[0]

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

        cur.close()

    return result


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
    q: Optional[str] = Query(None, description="Full-text search query (matches tweet text or username)"),
    usernames: Optional[str] = Query(None, description="Comma-separated list of authors to filter by"),
    area: Optional[str] = Query(None, description="Geographic area name to filter by"),
    aggressor: Optional[str] = Query(None, description="Filter by aggressor country name"),
    weapon_type: Optional[List[str]] = Query(None, description="Filter by weapon(s) used. Repeat the param for multiple values, e.g. ?weapon_type=A&weapon_type=B"),
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
    conditions = ["T.CREATED_AT >= NOW() - INTERVAL '30 days'"]
    params = []

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
    
    if weapon_type:
        placeholders = ','.join(['%s'] * len(weapon_type))
        conditions.append(f"ma.weapon_type IN ({placeholders})")
        params.extend(weapon_type)

    where_clause = " AND ".join(conditions)

    query = f"""
    WITH filtered_tweets AS (
        SELECT
            t.tweet_id, t.tweet_url, t.username, t.created_at, t.text,
            t.location_accuracy, t.nominatim_query, t.geom,
            t.importance_score, t.conflict_typology, t.verified,
            t.location_source, t.is_duplicate, t.fk_topic
        FROM tweets t
        WHERE {where_clause}
          AND t.is_duplicate = 'false'
          AND t.geom IS NOT NULL
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

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(query, params)

        geojson_data = cur.fetchone()[0] or {
            "type": "FeatureCollection",
            "features": []
        }
        cur.close()

    return Response(content=json.dumps(geojson_data), media_type="application/geo+json")


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
        ORDER BY CREATED_AT DESC
        LIMIT 15;
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


@app.get("/military_lines.geojson")
def get_military_lines(
    start_date: datetime = Query(..., description="Start date (e.g. 2026-02-14T00:00:00Z)"),
    end_date: datetime = Query(..., description="End date (e.g. 2026-02-15T23:59:59Z)")
):
    """
    Returns military action lines (aggressor -> target) as a GeoJSON FeatureCollection,
    over a given time range.

    Args:
        start_date (datetime): Start date (with timezone) - REQUIRED
        end_date (datetime): End date (with timezone) - REQUIRED
    """
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                JSON_BUILD_OBJECT(
                    'type', 'FeatureCollection',
                    'features', JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'type', 'Feature',
                            'geometry', ST_ASGEOJSON(ST_MAKELINE(AGGRESSOR_GEOM, T.GEOM))::JSON,
                            'properties', JSON_BUILD_OBJECT(
                                'created_at', T.CREATED_AT,
                                'label', TOP.LABEL,
                                'weapon_type',weapon_type,
                                'objective_type',objective_type
                            )
                        )
                    )
                )
            FROM
                MILITARY_ACTIONS MA
                NATURAL JOIN TWEETS T
                LEFT JOIN TOPICS TOP ON TOP.TOPIC_ID = T.FK_TOPIC
            WHERE
                T.CREATED_AT >= NOW() - INTERVAL '30 days'
            """,
            (start_date, end_date)
        )

        geojson_data = cur.fetchone()[0]
        cur.close()

    return Response(content=json.dumps(geojson_data), media_type="application/geo+json")


@app.get("/graph_events")
def get_graph_events(
    start_date: Optional[datetime] = Query(None, description="Start date (e.g. 2026-02-14T00:00:00Z). Defaults to 30 days before end_date."),
    end_date: Optional[datetime] = Query(None, description="End date (e.g. 2026-02-15T23:59:59Z). Defaults to now."),
    weapon_type: Optional[List[str]] = Query(None, description="Filter by weapon(s) used. Repeat the param for multiple values, e.g. ?weapon_type=A&weapon_type=B"),
    objective_type: Optional[List[str]] = Query(None, description="Filter by objective(s) targeted. Repeat the param for multiple values, e.g. ?objective_type=A&objective_type=B"),
):
    conditions = ["T.CREATED_AT >= NOW() - INTERVAL '30 days'"]
    params = []

    if end_date is None:
        end_date = datetime.utcnow()

    if start_date is None:
        start_date = end_date - timedelta(days=30)

    if weapon_type:
        placeholders = ','.join(['%s'] * len(weapon_type))
        conditions.append(f"MA.weapon_type IN ({placeholders})")
        params.extend(weapon_type)

    if objective_type:
        placeholders = ','.join(['%s'] * len(objective_type))
        conditions.append(f"MA.objective_type IN ({placeholders})")
        params.extend(objective_type)

    where_clause = " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                D.DAY AS DATE,
                COUNT(T.CREATED_AT) AS EVENTS
            FROM
                GENERATE_SERIES(
                    %s::TIMESTAMP,
                    %s::TIMESTAMP,
                    INTERVAL '24 hours'
                ) AS D (DAY)
                LEFT JOIN TWEETS T ON T.CREATED_AT >= D.DAY
                AND T.CREATED_AT < D.DAY + INTERVAL '24 hours'
                AND T.GEOM IS NOT NULL
                AND T.IS_DUPLICATE = 'false'
                LEFT JOIN MILITARY_ACTIONS MA ON T.TWEET_ID = MA.TWEET_ID
                WHERE {where_clause} and FK_TOPIC IS NOT NULL
            GROUP BY
                D.DAY
            ORDER BY
                D.DAY ASC;
            """,
            [start_date, end_date] + params
        )
        graph_events = cur.fetchall()
        cur.close()

    return {
        "events": [
            {"date": date.isoformat(), "count": events}
            for date, events in graph_events
        ]
    }