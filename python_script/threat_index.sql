DROP VIEW IF EXISTS v_country_tension_score;
CREATE VIEW v_country_tension_score AS
WITH
    WEIGHTS AS (
        SELECT 'MIL' AS CONFLICT_TYPOLOGY, 1.0 AS TYPE_WEIGHT UNION ALL
        SELECT 'MOVE',                      0.6              UNION ALL
        SELECT 'POL',                       0.3              UNION ALL
        SELECT 'OTHER',                     0.2
    ),
    EVENTS_WITH_COUNTRY AS (
        SELECT
            T.TWEET_ID,
            T.IMPORTANCE_SCORE,
            T.CONFLICT_TYPOLOGY,
            T.CREATED_AT,
            WA.ENTITY_NAME AS COUNTRY,
            'location' AS ROLE
        FROM TWEETS T
        JOIN WORLD_AREAS WA ON ST_WITHIN(T.GEOM, WA.GEOM)
        WHERE T.CREATED_AT >= NOW() - INTERVAL '7 days'
          AND T.GEOM IS NOT NULL
          AND T.LOCATION_ACCURACY != 'low'
          AND WA.ENTITY_TYPE = 'country'
    ),
    AGGRESSORS AS (
        SELECT
            T.TWEET_ID,
            T.IMPORTANCE_SCORE,
            T.CONFLICT_TYPOLOGY,
            T.CREATED_AT,
            MA.AGGRESSOR AS COUNTRY,
            'aggressor' AS ROLE
        FROM TWEETS T
        JOIN MILITARY_ACTIONS MA USING (TWEET_ID)
        WHERE T.CREATED_AT >= NOW() - INTERVAL '7 days'
          AND MA.AGGRESSOR IS NOT NULL
    ),
    TARGETS AS (
        SELECT
            T.TWEET_ID,
            T.IMPORTANCE_SCORE,
            T.CONFLICT_TYPOLOGY,
            T.CREATED_AT,
            MA.TARGET AS COUNTRY,
            'target' AS ROLE
        FROM TWEETS T
        JOIN MILITARY_ACTIONS MA USING (TWEET_ID)
        WHERE T.CREATED_AT >= NOW() - INTERVAL '7 days'
          AND MA.TARGET IS NOT NULL
    ),
    ALL_EVENTS AS (
        SELECT * FROM EVENTS_WITH_COUNTRY
        UNION ALL
        SELECT * FROM AGGRESSORS
        UNION ALL
        SELECT * FROM TARGETS
    ),
    DEDUPED_EVENTS AS (
        SELECT
            COUNTRY,
            TWEET_ID,
            MAX(IMPORTANCE_SCORE)  AS IMPORTANCE_SCORE,
            MAX(CREATED_AT)        AS CREATED_AT,
            MAX(CONFLICT_TYPOLOGY) AS CONFLICT_TYPOLOGY,
            MAX(CASE ROLE
                WHEN 'aggressor' THEN 3
                WHEN 'target'    THEN 2
                WHEN 'location'  THEN 1
            END) AS ROLE_RANK
        FROM ALL_EVENTS
        GROUP BY COUNTRY, TWEET_ID
    ),
    SCORED_EVENTS AS (
        SELECT
            DE.COUNTRY,
            DE.TWEET_ID,
            DE.ROLE_RANK,
            DE.IMPORTANCE_SCORE,
            CASE DE.ROLE_RANK
                WHEN 3 THEN 3.0
                WHEN 2 THEN 2.5
                WHEN 1 THEN 0.2
            END AS ROLE_WEIGHT,
            EXP(
                -0.693 * EXTRACT(EPOCH FROM (NOW() - DE.CREATED_AT)) / (7 * 86400)
            ) AS RECENCY_WEIGHT,
            W.TYPE_WEIGHT
        FROM DEDUPED_EVENTS DE
        JOIN WEIGHTS W ON W.CONFLICT_TYPOLOGY = DE.CONFLICT_TYPOLOGY
    ),
    COUNTRY_RAW AS (
        SELECT
            COUNTRY,
            SUM(IMPORTANCE_SCORE * RECENCY_WEIGHT * TYPE_WEIGHT * ROLE_WEIGHT) AS RAW_SCORE,
            COUNT(DISTINCT TWEET_ID)                                            AS EVENT_COUNT,
            MAX(IMPORTANCE_SCORE)                                               AS MAX_SEVERITY,
            COUNT(DISTINCT CASE WHEN ROLE_RANK = 3 THEN TWEET_ID END)          AS ATTACKS_LAUNCHED,
            COUNT(DISTINCT CASE WHEN ROLE_RANK = 2 THEN TWEET_ID END)          AS ATTACKS_RECEIVED
        FROM SCORED_EVENTS
        GROUP BY COUNTRY
    ),
    NORMALIZED AS (
        SELECT
            CR.COUNTRY,
            CR.EVENT_COUNT,
            CR.MAX_SEVERITY,
            CR.ATTACKS_LAUNCHED,
            CR.ATTACKS_RECEIVED,
            CR.RAW_SCORE,
            LEAST(
                ROUND((LN(1 + CR.RAW_SCORE) / NULLIF(LN(1 + C.MAX_SCORE), 0)) * 60)
                + LEAST(ROUND((CR.ATTACKS_LAUNCHED + CR.ATTACKS_RECEIVED) / 10.0 * 40), 40),
                100
            ) AS TENSION_SCORE
        FROM COUNTRY_RAW CR
        CROSS JOIN (SELECT MAX(RAW_SCORE) AS MAX_SCORE FROM COUNTRY_RAW) C
    )
SELECT
    COUNTRY,
    TENSION_SCORE,
    EVENT_COUNT,
    ATTACKS_LAUNCHED,
    ATTACKS_RECEIVED,
	RAW_SCORE, 
    MAX_SEVERITY,  
    CASE
        WHEN TENSION_SCORE >= 80 THEN 'Open warfare'
        WHEN TENSION_SCORE >= 60 THEN 'Active conflict'
        WHEN TENSION_SCORE >= 40 THEN 'High tension'
        WHEN TENSION_SCORE >= 20 THEN 'Moderate tension'
        ELSE                          'Calm'
    END AS THREAT_LEVEL
FROM NORMALIZED
ORDER BY TENSION_SCORE DESC;
