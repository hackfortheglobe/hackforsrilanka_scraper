from calendar import c
from pickle import FALSE, TRUE
from lxml import html
import requests
import camelot
import pandas as pd
from datetime import datetime
import pytz
import json
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import logging.config
import yaml
import os
import sys
import time

start_time = time.time()

# Init logging into file and console
with open('logging_config.yml', 'r') as config:
    logging.config.dictConfig(yaml.safe_load(config))
logger = logging.getLogger(__name__)

logging.info('Scraper init')

lastIdStorage = './assets/last_ceb_filename.txt'

# Init scheduler
sched = BlockingScheduler()

# Get Sri Lanka local time
sl_time = datetime.now(pytz.timezone('Asia/Colombo')).strftime('%Y-%m-%d')

def get_target_url():
    # request, bypass certificate check
    req = requests.get('https://ceb.lk', verify=False)
    webpage = html.fromstring(req.content)
    links = webpage.xpath('//a/@href')
    # look for google drive link
    gd_link = [i for i in links if 'drive.google.com' in i]
    # remove duplicates
    gd_link = list(set(gd_link))
    return gd_link[0]

def validate_target_id(currentId):
    # Check if the file is present and their content is not currentId
    if os.path.isfile(lastIdStorage):
        text_file = open(lastIdStorage, "r")
        lastId = text_file.read()
        text_file.close()
        if (lastId == currentId):
            return False
    # Is valid: return True
    return True

def save_last_id_processed(currentId):
    # Override the content of lastIdStorage for next validate_target_id()
    with open(lastIdStorage,'w') as f:
        f.write(currentId)

def convert_time(time_str, time_date = sl_time):
    time_str = time_date+'T'+time_str+':00.000Z'
    return time_str

def get_new_destination_path():
    now = datetime.now()
    formatedDate = time.strftime("%y-%m-%d_%H:%M:%S")
    destinationPath = "./assets/ceb_%s.pdf" % (formatedDate)
    return destinationPath

def download_file_from_google_drive(id, destination):
    URL = "https://docs.google.com/uc?export=download"
    session = requests.Session()
    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = get_confirm_token(response)
    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)
    save_response_content(response, destination)
    
def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value
    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768
    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

def process_tables(tables):
    logging.info("Processing Tables")
    dff = pd.DataFrame()
    for ii in range(2):
        df = tables[ii].df
        df = pd.DataFrame(data=df.iloc[1:].values, columns=df.iloc[0])
        df['starting_period'] = df['Period'].apply(lambda x: x.split(' ')[0] if ' ' in x else x.split('-')[0])
        df['ending_period'] = df['Period'].apply(lambda x: x.split(' ')[-1] if ' ' in x else x.split('-')[-1])
        df['starting_period'] = df['starting_period'].apply(lambda x: x.replace('–', '')).apply(lambda x: convert_time(x))
        df['ending_period'] = df['ending_period'].apply(lambda x: x.replace('–', '')).apply(lambda x: convert_time(x))

        for jj in range(len(df)):
            eles = df['Schedule Group'][jj].replace(' ', '').split(',')
            eles = ' '.join(eles).split()
            for ele in eles:
                dff = dff.append(pd.DataFrame(data={'group_name': ele, 'starting_period': [df.iloc[jj]['starting_period']],
                                                    'ending_period': [df.iloc[jj]['ending_period']]
                                                    }))
        return dff

def logFinish(reason):
    logging.info("========> %s" % (reason))
    logging.info("========> %s seconds" % (time.time() - start_time))
    logging.info("")

if __name__ == "__main__":
    logging.info('Scraper start')

    # Get the Google Docs url
    targetUrl = get_target_url()
    logging.info("Target Google Docs URL:" + targetUrl)

    # Validate not processed
    targetId = targetUrl.split('/')[5]
    isValidId = validate_target_id(targetId)
    if not isValidId:
        logFinish("Skipping target, this file is already processed")
        sys.exit()

    logging.info("Detected new document to process")    

    # Download the Google Docs
    destination = get_new_destination_path()
    logging.info("Saving Google Doc into " + destination)
    download_file_from_google_drive(targetId, destination)
    
    # Extract the data from the file
    tables = camelot.read_pdf(destination)
    # convert to json format {"group":..., "start_time":..., "end_time":...}
    json_out = process_tables(tables).reset_index(drop=True).to_json(orient='records')
    dict_obj = {"schedules": json.loads(json_out)}
    api_url = 'https://hackforsrilanka-api.herokuapp.com/api/illuminati/data'

	# Log extracted data
    data_size = len(json_out)
    logging.info("Obtained %s new squedules" % (data_size))
    logging.info(dict_obj)
    
    if not 'POST_TO_API' in os.environ or os.get('POST_TO_API') != 'true':
        # Skipping the post
        logging.info("Skipping data post to API")
        logFinish("Skipped post of %s entries" % (data_size))
    else:
        # Post the scraped data into our API 
        logging.info("Post data to API at: " + api_url)
        response = requests.post(api_url, json=dict_obj)
        
        # Log the response from API
        logging.info("Response code: " + str(response.status_code))
        logging.info("Response reason: " + response.reason)
        logging.info("Response content: " + str(response.content))

        if (response.status_code == 200):
            save_last_id_processed(targetId)
            logFinish("Data posted successfully (%s entries" % (data_size))
        else:
            logging.error("Error posting data")
            logFinish("Error posting data")
	
