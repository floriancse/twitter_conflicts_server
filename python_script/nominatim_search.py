import requests

def nominatim_geolocation(q):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": q,
        "format": "geojson",
        "language": "en"
    }
    headers = {'User-Agent': 'osint-observer-geolocation'}

    r = requests.get(url, headers=headers, params=params)
    features = r.json().get("features", [])

    if not features:
        return None

    directional_keywords = [
    'Middle East',
    'Eastern', 'Northern', 'Western', 'Southern', 'Central',
    'North', 'South', 'East', 'West'
    ]

    if any(keyword.lower() in q.lower() for keyword in directional_keywords):
        return None

    data_sorted = sorted(features, key=lambda feature: feature['properties']['importance'], reverse=True)
    geo = data_sorted[0]
    props = geo["properties"]

    if props["importance"] > 0.1 and props["place_rank"] < 26:
        geometry = geo["geometry"]

        if geometry["type"] == "Point":
            lon, lat = geometry["coordinates"]
        else:
            bbox = props.get("bbox")
            if not bbox:
                return None
            lon = (bbox[0] + bbox[2]) / 2
            lat = (bbox[1] + bbox[3]) / 2

        return [lat, lon]

    return None

if __name__ == "__main__":
    print(nominatim_geolocation("Eastern France"))