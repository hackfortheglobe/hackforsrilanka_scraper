from datetime import datetime
from main import get_dates, extract_data
import os
from pathlib import Path
import json
import pandas as pd

# testExtractionBatch.py
# 
# This test performs the extraction of schedules and places over a collection of PDF files. 
# It also output a summary file and export the data extracted for each pdf in JSON and in CSV.
#

base = Path(__file__).resolve().parent

summaryPath = os.path.join(base,'outputs',"summary.txt")

pdfFileNames = ["ceb_2022-04-09", 
    "ceb_2022-04-11to15",
    "ceb_2022-04-18",
    "ceb_2022-04-25to27",
    "ceb_2022-04-28to30",
    "ceb_2022-05-01to04",
    "ceb_2022-05-05",
    "ceb_2022-05-06to08"]

def add_to_summary(text_to_append):
    print(text_to_append)
    """Append given text as a new line at the end of file"""
    # Open the file in append & read mode ('a+')
    with open(summaryPath, "a+") as file_object:
        # Move read cursor to the start of file.
        file_object.seek(0)
        # If file is not empty then append '\n'
        data = file_object.read(100)
        if len(data) > 0:
            file_object.write("\n")
        # Append text at the end of file
        file_object.write(text_to_append)

summary = "Starting testPlacesBatch.py for %s files" % (len(pdfFileNames))
add_to_summary(summary)
start_test = datetime.now()
  
for pdfFileName in pdfFileNames:
    # Prepare file
    add_to_summary("Processing %s" % (pdfFileName))
    pdfPath = os.path.join(base,'assets', pdfFileName + '.pdf')

    # Extract dates
    informed_days = get_dates(pdfPath)
    add_to_summary(" - Informed days are %s : %s" % (len(informed_days), informed_days))

    # Extract data
    start_extraction = datetime.now()
    extracted_data = extract_data(pdfPath)
    duration = datetime.now().timestamp() - start_extraction.timestamp()
    json_places = extracted_data[0]
    json_schedules = extracted_data[1]
    add_to_summary(" - Extracted data in %s seconds" % (duration))

    # Print extracted places
    gss_count = len(json_places)
    area_count = 0
    for gss_name in json_places.keys():
        current_gss_areas = len(json_places[gss_name])
        area_count = area_count + current_gss_areas
    add_to_summary(" - Extracted places: %s areas in %s gss" % (area_count, gss_count))

    # Print extracted schedules
    schedules_count = len(json_schedules["schedules"])
    add_to_summary(" - Extracted schedules: %s power cuts" % (schedules_count))

    # Save extracted places in JSON
    jsonPath = os.path.join(base,'outputs', pdfFileName + "_places" + '.json')
    with open(jsonPath, "w") as outfile:
        json.dump(json_places, outfile)

    # Save extracted places in CSV
    csvPath = os.path.join(base,'outputs', pdfFileName + "_places" + '.csv')
    data = []
    for gss_name in json_places.keys():
        for area_name in json_places[gss_name].keys():
            current_area = json_places[gss_name][area_name]
            for group_name in current_area["groups"]:
                row = {"gss": gss_name, "area": area_name, "group": group_name}
                data.append(row)
    df = pd.DataFrame(data)
    df.to_csv (csvPath, index = None)

    # Save extracted schedules in JSON
    jsonPath = os.path.join(base,'outputs', pdfFileName + "_schedules" + '.json')
    with open(jsonPath, "w") as outfile:
        json.dump(json_schedules, outfile)

    # Save extracted schedules in CSV
    csvPath = os.path.join(base,'outputs', pdfFileName + "_schedules" + '.csv')
    data = []
    for schedule in json_schedules["schedules"]:
        row = {"group_name": schedule["group_name"], "starting_period": schedule["starting_period"], "endinging_period": schedule["starting_period"]}
        data.append(row)
    df = pd.DataFrame(data)
    df.to_csv (csvPath, index = None)


duration = datetime.now().timestamp() - start_test.timestamp()
add_to_summary("Finished testPlaces.py in %s seconds" % (duration))