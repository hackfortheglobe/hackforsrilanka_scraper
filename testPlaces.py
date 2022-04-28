from main import extract_places
import os
from pathlib import Path
import json

base = Path(__file__).resolve().parent
loc = os.path.join(base,'assets','ceb_2022-04-11to15.pdf')
json_places = extract_places(loc)

gss_count = len(json_places)
area_count = 0
for gss_name in json_places.keys():
    current_gss_areas = len(json_places[gss_name])
    area_count = area_count + current_gss_areas
print("Obtained places: %s areas in %s gss" % (area_count, gss_count))


with open("places_data.json", "w") as outfile:
    json.dump(json_places, outfile)