from main import extract_places
import os
from pathlib import Path
import json
import pandas as pd

base = Path(__file__).resolve().parent
loc = os.path.join(base,'assets','ceb_2022-04-25-27.pdf')
print(extract_places(loc)[1])
# json_places = extract_places(loc)
#
# gss_count = len(json_places)
# area_count = 0
# for gss_name in json_places.keys():
#     current_gss_areas = len(json_places[gss_name])
#     area_count = area_count + current_gss_areas
# print("Obtained places: %s areas in %s gss" % (area_count, gss_count))
#
#
# with open("places_data.json", "w") as outfile:
#     json.dump(json_places, outfile)
#
# data = []
# for gss_name in json_places.keys():
#     for area_name in json_places[gss_name].keys():
#         current_area = json_places[gss_name][area_name]
#         for group_name in current_area["Group"]:
#             row = {"GSS": gss_name, "AREA": area_name, "GROUP": group_name}
#             data.append(row)
#
# df = pd.DataFrame(data)
# df.to_csv ("places_data.csv", index = None)
