WITH
	BORDERS AS (
		SELECT
			A.entity_name AS COUNTRY_A,
			B.entity_name AS COUNTRY_B,
			ST_LINEMERGE (
				ST_COLLECTIONEXTRACT (ST_INTERSECTION (A.GEOM, B.GEOM), 2)
			) AS GEOM_LINE,
			ST_BUFFER (
				ST_LINEMERGE (
					ST_COLLECTIONEXTRACT (ST_INTERSECTION (A.GEOM, B.GEOM), 2)
				),
				.2,
				'join=mitre endcap=flat'
			) AS GEOM_BUFFER
		FROM
			WORLD_AREAS A
			JOIN WORLD_AREAS B ON A.entity_name < B.entity_name
			AND ST_INTERSECTS (A.GEOM, B.GEOM)
			JOIN TENSION_INDEX_MV TA ON TA.COUNTRY = A.entity_name
			JOIN TENSION_INDEX_MV TB ON TB.COUNTRY = B.entity_name
		WHERE
			TA.TENSION_SCORE > 50
			AND TB.TENSION_SCORE > 50
			AND ST_GEOMETRYTYPE (ST_INTERSECTION (A.GEOM, B.GEOM)) != 'ST_Polygon'
	)
SELECT
	-- GeoJSON des lignes
	(
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
						ST_ASGEOJSON (GEOM_LINE)::JSON,
						'properties',
						JSON_BUILD_OBJECT('country_a', COUNTRY_A, 'country_b', COUNTRY_B)
					)
				)
			)
		FROM
			BORDERS
	) AS GEOJSON_LINES,
	-- GeoJSON des buffers
	(
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
						ST_ASGEOJSON (GEOM_BUFFER)::JSON,
						'properties',
						JSON_BUILD_OBJECT('country_a', COUNTRY_A, 'country_b', COUNTRY_B)
					)
				)
			)
		FROM
			BORDERS
	) AS GEOJSON_BUFFERS;