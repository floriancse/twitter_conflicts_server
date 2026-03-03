from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import timedelta
from math import radians, sin, cos, sqrt, atan2

SQL_GET_RECENT_TWEETS_FOR_DEDUP = """
    SELECT
        tweet_id,
        summary_text,
        text,
        created_at,
        conflict_typology,
        ST_Y(geom::geometry) AS lat,
        ST_X(geom::geometry) AS lon
    FROM   tweets
    WHERE  created_at >= NOW() - INTERVAL '24 hours'
      AND  geom IS NOT NULL
    ORDER BY created_at DESC
"""


def haversine(coord1, coord2):
    """Distance en km entre deux points (lat, lon). Retourne inf si l'un des points est None."""
    if coord1 is None or coord2 is None:
        return float('inf')
    R = 6371
    lat1, lon1 = radians(coord1[0]), radians(coord1[1])
    lat2, lon2 = radians(coord2[0]), radians(coord2[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))


def fetch_dedup_data(cur):
    """Récupère les tweets des dernières 24h ayant une géométrie pour la déduplication."""
    cur.execute(SQL_GET_RECENT_TWEETS_FOR_DEDUP)
    return cur.fetchall()


def delete_dup_rows(cur, conn):
    """
    Détecte et supprime les tweets en doublon parmi les tweets récents.
    Deux tweets sont considérés comme doublons s'ils partagent la même typologie,
    ont été publiés dans une fenêtre de 24h, et sont soit textuellement très proches,
    soit textuellement proches ET géographiquement proches.
    """
    rows = fetch_dedup_data(cur)
    if not rows:
        return

    # Extraction des champs utiles depuis les résultats de la requête
    ids        = [r[0] for r in rows]
    texts      = [r[1] for r in rows]
    timestamps = [r[3] for r in rows]
    typologies = [r[4] for r in rows]
    coords     = [(r[5], r[6]) if r[5] is not None else None for r in rows]

    # Calcul des embeddings et de la matrice de similarité cosinus
    model      = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    embeddings = model.encode(texts, show_progress_bar=True)
    sim_matrix = cosine_similarity(embeddings)

    # Seuils de détection des doublons
    SIM_THRESHOLD     = 0.80          # similarité textuelle seule
    SIM_THRESHOLD_GEO = 0.80          # similarité textuelle + proximité géographique
    GEO_RADIUS_KM     = 50            # rayon de proximité géographique
    TIME_WINDOW       = timedelta(hours=24)

    # Regroupement des tweets similaires par clustering naïf
    visited = set()
    groups  = []

    for i in range(len(ids)):
        if i in visited:
            continue
        group = [i]
        visited.add(i)

        for j in range(i + 1, len(ids)):
            if j in visited:
                continue

            same_type = typologies[i] == typologies[j]
            time_diff = abs(timestamps[i] - timestamps[j])
            is_close  = time_diff <= TIME_WINDOW
            sim       = sim_matrix[i][j]
            dist_km   = haversine(coords[i], coords[j])
            is_geo    = dist_km < GEO_RADIUS_KM

            is_dup = (
                same_type and is_close and (
                    sim >= SIM_THRESHOLD or                    # texte très similaire
                    (sim >= SIM_THRESHOLD_GEO and is_geo)      # texte similaire + même zone géo
                )
            )

            if is_dup:
                group.append(j)
                visited.add(j)

        groups.append(group)

    # Identification des groupes contenant au moins un doublon
    duplicate_groups     = [g for g in groups if len(g) > 1]
    duplicates_to_delete = [ids[idx] for group in duplicate_groups for idx in group[1:]]

    # Suppression en base — on conserve le premier tweet de chaque groupe
    if duplicates_to_delete:
        cur.execute(
            "DELETE FROM tweets WHERE tweet_id = ANY(%s)",
            (duplicates_to_delete,)
        )
        conn.commit()