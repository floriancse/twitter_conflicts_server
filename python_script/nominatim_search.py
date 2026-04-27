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

    if q in ['Middle East', 'South Lebanon', 'Palestine', 'Washington, United States', 'Washington, USA', 'New York, United States']:
        return None

    data_sorted = sorted(features, key=lambda feature: feature['properties']['place_rank'])
    geo = data_sorted[0]
    props = geo["properties"]
    
    if props["place_rank"] <= 20 and props["importance"] > 0.1:
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
    print(nominatim_geolocation(" Bab al-Mandab Strait "))