import json

orientation_dict = {
  'north': 'N', 'northwest': 'NW', 'west': 'W', 'southwest': 'SW',
  'south': 'S', 'southeast': 'SE', 'east': 'E', 'northeast': 'NE'
}

def apply_correction(location):
	with open("twitter_conflicts_server/python_script/nswe.geojson", "r") as file:
		data = json.load(file)

	
	parts = location.split(", ")
	country = parts[-1]
	orientation = parts[0].split(" ")[0].replace("ern", "").lower()
	orientation = orientation_dict[orientation]

	for i in data["features"]:
		if i["properties"]["country"] == country and i["properties"]["direction"] == orientation:
			lat, long = i["geometry"]["coordinates"]
			lat, long = round(lat, 2), round(long, 2)
			return [lat, long]

if __name__ == "__main__":
    result = apply_correction("Eastern Democratic Republic of the Congo, Democratic Republic of the Congo")
    print(result)