from calendar import c
from pickle import FALSE, TRUE
from lxml import html
import requests
import camelot
import pandas as pd
from datetime import datetime
import pytz
import json
import logging
import logging.config
import yaml
import os
import sys
import time
import re
from pathlib import Path
import string

from apscheduler.schedulers.blocking import BlockingScheduler
from storage import Storage

start_datetime = datetime.now()

# Init logging into file and console
with open('logging_config.yml', 'r') as config:
    logging.config.dictConfig(yaml.safe_load(config))
logger = logging.getLogger(__name__)
logger.info('Scraper init')

# Init scheduler
sched = BlockingScheduler()

# Init dev_mode variable
if not 'POST_TO_API' in os.environ or os.environ.get('POST_TO_API') != 'true':
    logger.info("Dev mode enabled (no push to api and no connections to ftp storage).")
    dev_mode = True
else:
    logger.info("Dev mode disabled (data will be pushed to our API and stored in our FTP storage).")
    dev_mode = False

# Init storage
storage = Storage(start_datetime, logger, dev_mode)

# Get Sri Lanka local time
sl_time = datetime.now(pytz.timezone('Asia/Colombo')).strftime('%Y-%m-%d')

def get_target_url():
    # request ceb.lk home page
    req = requests.get('https://ceb.lk', verify=False)
    webpage = html.fromstring(req.content)
    links = webpage.xpath('//a/@href')
    
    # look for google drive links
    gd_link = [i for i in links if 'drive.google.com' in i]
    gd_link = list(set(gd_link))
    if len(gd_link)>0:
        return gd_link[0]

    # look for bit.ly links and resolve it
    bl_link = [i for i in links if 'bit.ly' in i]
    bl_link = list(set(bl_link))
    if len(bl_link)>0:
        logger.info("Resolving Bit.ly: %s" % (bl_link[0]))
        req = requests.get(bl_link[0] + '+', verify=False)
        resolved_gd_link = re.search("\"long_url\": \"(.*)\", \"user_hash\": \"", str(req.content)).group(1)
        logger.info("Resolved Bit.ly: %s" % (resolved_gd_link))
        return resolved_gd_link

    logging.error("Not Google Drive link or Bit.ly links founded")
    logFinish("Unable to find document url")

def convert_time(time_str, time_date = sl_time):
    time_str = time_date+'T'+time_str+':00.000Z'
    return time_str

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

def extract_schedules(localDocPath):
    tables = camelot.read_pdf(localDocPath)
    dataframe = process_tables(tables)
    # convert to json format {"group":..., "start_time":..., "end_time":...}
    json_out = dataframe.reset_index(drop=True).to_json(orient='records')
    return json_out

def process_tables(tables):
    logger.info("Processing Tables")
    dff = pd.DataFrame()
    for ii in range(2):
        df = tables[ii].df
        df = pd.DataFrame(data=df.iloc[1:].values, columns=df.iloc[0])
        df['starting_period'] = df['Period'].apply(lambda x: x.split(' ')[0] if ' ' in x else x.split('-')[0])
        df['ending_period'] = df['Period'].apply(lambda x: x.split(' ')[-1] if ' ' in x else x.split('-')[-1])
        df['starting_period'] = df['starting_period'].apply(lambda x: x.replace('–', '')).apply(lambda x: convert_time(x))
        df['ending_period'] = df['ending_period'].apply(lambda x: x.replace('–', '')).apply(lambda x: convert_time(x))

        for jj in range(len(df)):
            if 'Schedule Group' in df.columns:
                column = df['Schedule Group']
            elif 'Group' in df.columns:
                column = df['Group']
            else:
                logging.error("Not found column called 'Schedule Group' or 'Group'")
                logFinish("Unable to parse the pdf tables, the structure could have changed")
                return

            eles = column[jj].replace(' ', '').split(',')
            eles = ' '.join(eles).split()
            for ele in eles:
                dff = dff.append(pd.DataFrame(data={'group_name': ele, 'starting_period': [df.iloc[jj]['starting_period']],
                                                    'ending_period': [df.iloc[jj]['ending_period']]
                                                    }))
        return dff

def extract_locations(pdf_dir):
    # reading the pdf file
    tables = camelot.read_pdf(pdf_dir,pages='all')
    # dictionary to store all tables in pandas dataframe with keys assigned as data0,data1 and so on..to process
    data_dic = {}
    # getting all the tables from pdf file
    for no in range(0,len(tables)):
        data_dic['data{}'.format(no)] = tables[no].df
    # It checks group info in each database by looping through database and each cell of that
    # here 'no' is dataFrame/table no & 'x' is column no
    all_groups = [] # all groups that are indexed in starting pages
    actual_groups = [] # Groups infs available in the cells of their table
    for no in range(0,len(data_dic)):
        for x in range(0,data_dic['data{}'.format(no)].shape[1]):
            if 'Group' in data_dic['data{}'.format(no)].iloc[0].values[x]:
                if no>5 :
                    actual_groups.append((no,x))
                else:
                    all_groups.append((no,x))
    # Setting columns for main(all) grouping data
    for y in range(0,len(all_groups)):
        data_dic['data{}'.format(y)].columns  = data_dic['data{}'.format(y)].iloc[0]
        data_dic['data{}'.format(y)].drop(0,inplace=True)
    # look for what groups we have by looping through columns data
    groups =[]
    for table_no in range(0,len(all_groups)):
        current_table = data_dic['data{}'.format(table_no)]
        for x in current_table.iloc[:,all_groups[1][1]].values:
            print(x)
            for letter in string.ascii_uppercase:
                if letter in x:
                    groups.append(letter)
    groups = [x for x in groups if x in groups]
    groups = set(groups)
    groups = sorted(groups)
    main_dict = {}
    group_count = 0

    print (groups)
    print(all_groups)
    print(data_dic)

    # settings indexes,removing extra row, assigning groups
    for table_no in range(len(all_groups),len(data_dic)):
        
        current_table = data_dic['data{}'.format(table_no)]
        print("Parsing table %s: %s", table_no, current_table)

        # it checks whether tht table has 3 cols
        # it rules out of adding unsymmetrical data to group which just got processed
        if current_table.shape[1] == 3:
            # it checks whether the data is countinuing for last group or data of new group
            col_check=[]
            for col in current_table.iloc[0].values:
                if 'GSS' in col:
                    col_check.append(True)
                elif 'Affected' in col:
                    col_check.append(True)
                elif 'Feeder' in col:
                    col_check.append(True)
                else:
                    col_check.append(False)
            # Starting a New Group
            if all(col_check):
                current_table.columns  = ['GSS','Feeder No','Affected area']
                current_table.drop(0,inplace=True)
                main_dict['Group {}'.format(groups[group_count])] = current_table
                last_group = 'Group {}'.format(groups[group_count])
                group_count+=1
            # Starting new group for data whose groups are not indexed on above pages
            if actual_groups:
                if table_no>=actual_groups[0][0] and table_no<=actual_groups[-1][0]:
                    current_table.columns  = ['GSS','Feeder No','Affected area']
                    main_dict[current_table.iloc[0].values[0]]= current_table[2:]
                    last_group = current_table.iloc[0].values[0]
            # it is concatenating continous data of last group(Note: one filter only which is.. it should have 3 columns
            else:
                current_table.columns = ['GSS','Feeder No','Affected area']
                main_dict[last_group] = pd.concat([main_dict[last_group],current_table])
    final={}
    # Resetting index and converting into dic
    for group,table in main_dict.items():
        table.reset_index(drop=True)
        table = table.to_dict('list')
        final[group] = table

    # Save into a file
    if dev_mode:
        with open('locations_data.json', 'w') as outfile:
            json.dump(final, outfile, indent=4)

    return final

def logFinish(reason):
    logger.info("========> %s" % (reason))
    logger.info("========> %s seconds" % (datetime.now().timestamp() - start_datetime.timestamp()))
    logger.info("")
    sys.exit()

if __name__ == "__main__":
    logger.info('Scraper start')

    # Get the Google Docs url
    targetUrl = get_target_url()
    logger.info("Target Google Docs URL:" + targetUrl)
    targetId = targetUrl.split('/')[5]

    # Donwload last processed id by ftp
    try:
        storage.download_last_processed()
    except Exception as e:
        logger.error(e)
        logFinish("Error downloading last processed Id")

    # Check if should continue processing
    isValidId = storage.validate_doc_id(targetId)
    if not isValidId:
        logFinish("Skipping target, this file is already processed")
    logger.info("Detected new document to process")    

    # Download the Google Docs
    localDocPath = storage.get_local_doc_path()
    logger.info("Saving Google Doc into " + localDocPath)
    download_file_from_google_drive(targetId, localDocPath)
    
    # Extract locations
    json_locations = extract_locations(localDocPath)
    data_size = len(json_locations)
    logger.info("Obtained %s new squedules" % (data_size))
    logger.info(json_locations)

    #TODO: Pushing locations to our API

    # Extract schedules
    json_schedules = extract_schedules(localDocPath)
    dict_obj = {"schedules": json.loads(json_schedules)}
    data_size = len(json_schedules)
    logger.info("Obtained %s new squedules" % (data_size))
    logger.info(dict_obj)


    # Pushing schedules to our API
    if dev_mode:
        # Skipping the post
        logger.info("Skipping data post to API")
        logFinish("Skipped post of %s entries" % (data_size))
    else:
        # Post the scraped data into our API
        api_url = 'https://hackforsrilanka-api.herokuapp.com/api/illuminati/data'
        logger.info("Post data to API at: " + api_url)
        response = requests.post(api_url, json=dict_obj)
        
        # Log the response from API
        logger.info("Response code: " + str(response.status_code))
        logger.info("Response reason: " + response.reason)
        logger.info("Response content: " + str(response.content))

        if (response.status_code == 200):
            storage.save_processed(targetId)
            logFinish("Data posted successfully (%s entries" % (data_size))
        else:
            logger.error("Error posting data")
            logFinish("Error posting data")
	
