import requests

def nominatim_geolocation(q):
    url = "https://nominatim.openstreetmap.org/search"
    data = {
        "q": q,
        "format": "geojson",
        "limit": 1,
        "language": "en"
    }
    headers = {'User-Agent': 'osint-observer-geolocation'}

    r = requests.get(url, headers=headers, params=data)
    features = r.json().get("features", [])

    if not features:
        return None

    geo = features[0]
    if geo["properties"]["importance"] > 0.3:
        lon = geo["geometry"]["coordinates"][0]
        lat = geo["geometry"]["coordinates"][1]
        return [lat, lon]

    return None
