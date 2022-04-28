from main import extract_locations
import os
from pathlib import Path
import json

base = Path(__file__).resolve().parent
loc = os.path.join(base,'assets','ceb_2022-04-11to15.pdf')
dict = extract_locations(loc)


with open("sample.json", "w") as outfile:
    json.dump(dict, outfile)
