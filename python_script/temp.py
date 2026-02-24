import requests

query = """
[out:json][timeout:300];
(
  node["military"="base"];
  way["military"="base"];
  relation["military"="base"];
);
out center tags;
"""

response = requests.get("https://overpass-api.de/api/interpreter", params={"data": query})
data = response.json()

# Extraire lat/lon
points = []
for el in data["elements"]:
    if el["type"] == "node":
        lat, lon = el.get("lat"), el.get("lon")
    else:
        center = el.get("center", {})
        lat, lon = center.get("lat"), center.get("lon")
    
    if lat and lon:
        name = el.get("tags", {}).get("name", "Base militaire")
        points.append((lat, lon, name))

print(f"{len(points)} bases trouvées")