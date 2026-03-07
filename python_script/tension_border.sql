SELECT
    ST_Union(WAR_BUFFER) AS WAR_ZONE
FROM (
    SELECT
        WAR_BUFFER,
        ST_ClusterDBSCAN(WAR_BUFFER, 0, 1) OVER () AS CLUSTER_ID
    FROM (
        SELECT
            ST_Intersection(
                ST_Buffer(ST_Intersection(A.GEOM, B.GEOM), 0.5),
                ST_Union(A.GEOM, B.GEOM)
            ) AS WAR_BUFFER
        FROM
            MILITARY_ACTIONS M
            LEFT JOIN WORLD_AREAS A ON M.AGGRESSOR = A.ENTITY_NAME
            LEFT JOIN WORLD_AREAS B ON M.TARGET = B.ENTITY_NAME
        WHERE
            TARGET IS NOT NULL
            AND ST_INTERSECTS(A.GEOM, B.GEOM)
        GROUP BY
            A.GEOM, B.GEOM
    ) SUB
) CLUSTERED
GROUP BY
    CLUSTER_ID