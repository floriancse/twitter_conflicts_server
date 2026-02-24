from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import timedelta
from math import radians, sin, cos, sqrt, atan2


def haversine(coord1, coord2):
    """Distance en km entre deux points (lat, lon). Retourne inf si l'un est None."""
    if coord1 is None or coord2 is None:
        return float('inf')
    R = 6371
    lat1, lon1 = radians(coord1[0]), radians(coord1[1])
    lat2, lon2 = radians(coord2[0]), radians(coord2[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

def delete_dup_rows(rows):
    if not rows:
        return
    ids        = [r[0] for r in rows]
    texts      = [f"{r[1] or ''} {r[2] or ''}" for r in rows]
    timestamps = [r[3] for r in rows]
    typologies = [r[4] for r in rows]
    coords     = [(r[5], r[6]) if r[5] is not None else None for r in rows]

    # Embeddings
    model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    embeddings = model.encode(texts, show_progress_bar=True)
    sim_matrix = cosine_similarity(embeddings)

    # Paramètres
    SIM_THRESHOLD     = 0.85  
    SIM_THRESHOLD_GEO = 0.75 
    GEO_RADIUS_KM     = 50    
    TIME_WINDOW       = timedelta(hours=24)

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

            same_type  = typologies[i] == typologies[j]
            time_diff  = abs(timestamps[i] - timestamps[j])
            is_close   = time_diff <= TIME_WINDOW
            sim        = sim_matrix[i][j]
            dist_km    = haversine(coords[i], coords[j])
            is_geo     = dist_km < GEO_RADIUS_KM

            is_dup = (
                same_type and is_close and (
                    sim >= SIM_THRESHOLD                        # texte très similaire
                    or (sim >= SIM_THRESHOLD_GEO and is_geo)   # texte moyennement similaire + même zone
                )
            )

            if is_dup:
                group.append(j)
                visited.add(j)

        groups.append(group)

    # Rapport
    duplicate_groups = [g for g in groups if len(g) > 1]
    flagged_ids      = set(idx for g in duplicate_groups for idx in g)
    duplicates_to_delete = [ids[idx] for group in duplicate_groups for idx in group[1:]]

    if duplicates_to_delete:
        cur.execute(
            "DELETE FROM tweets WHERE tweet_id = ANY(%s)",
            (duplicates_to_delete,)
        )
        conn.commit()
        print(f"\n{cur.rowcount} entrées supprimées")

