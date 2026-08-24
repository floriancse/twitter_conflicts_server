import requests
from math import radians, sin, cos, sqrt, atan2


def haversine(lat1, lon1, lat2, lon2):
    """Distance en km entre deux points (lat/lon en degrés)."""
    R = 6371.0
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))


def nominatim_geolocation_closest(q, ref_lat: float, ref_long: float, limit=50):
    """
    Interroge Nominatim pour tous les résultats correspondant à `q`
    et renvoie le point [lat, lon] le plus proche de (ref_lat, ref_long).
    """
    directional_keywords = [
        'Middle East',
        'Eastern', 'Northern', 'Western', 'Southern', 
        'North', 'South', 'East', 'West'
    ]

    directional_exceptions = [
        'north sea',
        'south china sea',
        'east china sea',
        'east sea',
        'southern ocean',
        'north channel',
        'western sahara',
        'northern ireland',
        'south sudan',
        'south africa',
        'south korea',
        'north korea',
        'north macedonia',
        'east timor',
        'western australia',  
        'North Waziristan, Pakistan',
        'North Kordofan, Sudan',
        'North Kivu, Democratic Republic of the Congo'
    ]

    if not q:
        return None

    q_lower = q.lower()

    is_exception = any(exc in q_lower for exc in directional_exceptions)

    if not is_exception and any(keyword.lower() in q_lower for keyword in directional_keywords):
        return q

    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": q,
        "format": "geojson",
        "language": "en",
        "limit": limit,
    }
    headers = {'User-Agent': 'osint-observer-geolocation'}

    r = requests.get(url, headers=headers, params=params)
    features = r.json().get("features", [])

    if not features:
        return None

    candidates = []
    for feature in features:
        geometry = feature["geometry"]
        props = feature["properties"]

        if geometry["type"] == "Point":
            lon, lat = geometry["coordinates"]
        else:
            bbox = props.get("bbox")
            if not bbox:
                continue
            lon = (bbox[0] + bbox[2]) / 2
            lat = (bbox[1] + bbox[3]) / 2

        if ref_lat is not None and ref_long is not None:
            dist = haversine(ref_lat, ref_long, lat, lon)
        else:
            dist = 0

        candidates.append((dist, lat, lon, props))

    if not candidates:
        return None

    if ref_lat is not None and ref_long is not None:
        candidates.sort(key=lambda c: c[0])

    dist, lat, lon, props = candidates[0]
    return [lat, lon]


if __name__ == "__main__":
    result = nominatim_geolocation_closest("North Korea", 62.0, 5.0)
    print(result)