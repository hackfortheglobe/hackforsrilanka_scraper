from datetime import datetime
from main import extract_places
import os
from pathlib import Path
import json
import pandas as pd

base = Path(__file__).resolve().parent

def append_new_line(file_name, text_to_append):
    """Append given text as a new line at the end of file"""
    # Open the file in append & read mode ('a+')
    with open(file_name, "a+") as file_object:
        # Move read cursor to the start of file.
        file_object.seek(0)
        # If file is not empty then append '\n'
        data = file_object.read(100)
        if len(data) > 0:
            file_object.write("\n")
        # Append text at the end of file
        file_object.write(text_to_append)

summaryPath = os.path.join(base,'outputs',"summary.txt")
pdfFileNames = ["ceb_2022-04-09", 
    "ceb_2022-04-11to15", 
    "ceb_2022-04-25to27", 
    "ceb_2022-04-28to30",
    "ceb_2022-05-01to04"]

summary = "Starting testPlaces.py for %s files" % (len(pdfFileNames))
print(summary)
append_new_line(summaryPath, summary)
start_test = datetime.now()
  
# Using for loop
for pdfFileName in pdfFileNames:
    print("Processing " + pdfFileName + '.pdf')
    pdfPath = os.path.join(base,'assets', pdfFileName + '.pdf')
    
    start_extraction = datetime.now()
    json_places = extract_places(pdfPath)
    duration = datetime.now().timestamp() - start_extraction.timestamp()

    gss_count = len(json_places)
    area_count = 0
    for gss_name in json_places.keys():
        current_gss_areas = len(json_places[gss_name])
        area_count = area_count + current_gss_areas

    summary = "%s places: %s areas at %s gss in %s seconds" % (pdfFileName + '.pdf', area_count, gss_count, duration)
    print(summary)
    append_new_line(summaryPath, summary)

    jsonPath = os.path.join(base,'outputs', pdfFileName + "_places" + '.json')
    with open(jsonPath, "w") as outfile:
        json.dump(json_places, outfile)

    csvPath = os.path.join(base,'outputs', pdfFileName + "_places" + '.csv')
    data = []
    for gss_name in json_places.keys():
        for area_name in json_places[gss_name].keys():
            current_area = json_places[gss_name][area_name]
            for group_name in current_area["Group"]:
                row = {"GSS": gss_name, "AREA": area_name, "GROUP": group_name}
                data.append(row)

    df = pd.DataFrame(data)
    df.to_csv (csvPath, index = None)

duration = datetime.now().timestamp() - start_test.timestamp()
summary = "Finished testPlaces.py in %s seconds" % (duration)
print(summary)
append_new_line(summaryPath, summary)