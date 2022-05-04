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
import numpy as np
from datetime import datetime as dt,timedelta
# pdfminer imports to extrcat dates from pdf
from io import StringIO
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser

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
sl_time = datetime.now(pytz.timezone('Asia/Colombo'))

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


# Get the dates affecting the schedule
def get_dates(localDocPath):
    months = ['January','February','March','April','May','June','July','August','September','October','November','December']
    dates_line = extract_dates_line(localDocPath)
    dates = re.findall(r'\b\d{2}\D',dates_line)
    months = re.findall('|'.join(months),dates_line)
    dates = list(map(lambda x: re.findall(r'\d{2}',x)[0],dates))
    
    if len(dates) == 1:
        range = [dt.strptime(f'{months[0][0:3]} {dates[0]} 2022','%b %d %Y')]
    else:
        if len(months)==1:
            start_date = dt.strptime(f'{months[0][0:3]} {dates[0]} 2022','%b %d %Y')
            end_date = dt.strptime(f'{months[0][0:3]} {dates[1]} 2022','%b %d %Y')
        else:
            start_date = dt.strptime(f'{months[0][0:3]} {dates[0]} 2022','%b %d %Y')
            end_date = dt.strptime(f'{months[1][0:3]} {dates[1]} 2022','%b %d %Y')
        range = get_dates_between(start_date,end_date)
        
    print("Document dates: ", range)
    return range

# Read the days affecting the schedule by reading the first line at the pdf
def extract_dates_line(localDocPath):
    output_string = StringIO()
    with open(localDocPath, 'rb') as in_file:
        parser = PDFParser(in_file)
        doc = PDFDocument(parser)
        rsrcmgr = PDFResourceManager()
        device = TextConverter(rsrcmgr, output_string, laparams=LAParams())
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        for page in PDFPage.create_pages(doc):
            interpreter.process_page(page)
    pdf_data= output_string.getvalue()
    dates_line = re.findall(r'Demand Management Schedule.+\b\d{2}\D+\b',pdf_data)[0]
    print ("Document title: ", dates_line)
    return dates_line

# Gives all days between two dates in datetime format
def get_dates_between(start, end):
    delta = end - start  # as timedelta
    days = [start + timedelta(days=i) for i in range(delta.days + 1)]
    return days

def old_extract_schedules(localDocPath):
    tables = camelot.read_pdf(localDocPath)

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

    # convert to json format {"group":..., "start_time":..., "end_time":...}
    json_out = dff.reset_index(drop=True).to_json(orient='records')
    return json_out

def extract_data(pdf_local_path):
    # Reading the pdf file
    tables = camelot.read_pdf(pdf_local_path,pages='all')

    # Prepare dictionary to store all tables in pandas dataframe with keys assigned as data0,data1 and so on..
    # for all tables found by camelot
    data_dic = {}
    # getting all the tables from pdf file
    for no in range(0,len(tables)):
        data_dic['data{}'.format(no)] = tables[no].df

    # Retrieve group info in each table dataFrame by looping through it. 'x' is column no, 'no' is table no
    all_groups = [] # all groups that are indexed in starting pages
    actual_groups = [] # Groups infs available in the cells of their table
    for no in range(0,len(data_dic)):
        for x in range(0,data_dic['data{}'.format(no)].shape[1]):
            if 'Group' in data_dic['data{}'.format(no)].iloc[0].values[x]:
                if no>5:
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
            for letter in string.ascii_uppercase:
                if letter in x:
                    groups.append(letter)
    groups = sorted(list(set(groups)))


    schedules = extract_schedule_data(data_dic,all_groups,groups,pdf_local_path)
    places = extract_places_data(data_dic,all_groups,groups,actual_groups)

    return [places,schedules]


def extract_schedule_data(data_dic,all_groups,groups,pdf_local_path):
    # converting schedues data from pdf to dictionary form
    schedules = {'schedules':[]}
    date_range = get_dates(pdf_local_path)
    for table_no in range(0,len(all_groups)):
        #passing rows of current table
        for index,row in data_dic['data{}'.format(table_no)].iterrows():
            joined_row = ' '.join(row.values)
            time_patt = re.compile(r'\s\d?\d.\d{2}\s')
            time_matches = time_patt.findall(joined_row)
            timings = [time_match for time_match in time_matches]
            if timings:
                groups = row[all_groups[1][1]].split(',')
                for group in groups:
                    for date in date_range:
                        schedules['schedules'].append({'group_name':group.strip(),
                        'starting_period':f'{date.strftime("%Y-%m-%d")} {timings[0].strip()}',
                        'ending_period':f'{date.strftime("%Y-%m-%d")} {timings[-1].strip()}'})

    # Save into a file for dev
    if dev_mode:
        with open('./outputs/extracted_schedules.json', 'w') as outfile:
            json.dump(schedules, outfile, indent=4)
    return schedules

def extract_places_data(data_dic,all_groups,groups,actual_groups):
    main_dict = {}
    group_count = 0
    # settings indexes,removing extra row, assigning groups
    for table_no in range(len(all_groups),len(data_dic)):
        # it checks whether tht table has 3 cols
        # it rules out of adding unsymmetrical data to group which just got processed

        if data_dic['data{}'.format(table_no)].shape[1] == 3:
            # it checks whether the data is countinuing for last group or data for new group
            col_check=[]
            for col in data_dic['data{}'.format(table_no)].iloc[0].values:
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
                data_dic['data{}'.format(table_no)].columns  = ['GSS','Feeder No','Affected area']
                data_dic['data{}'.format(table_no)].drop(0,inplace=True)
                main_dict['Group {}'.format(groups[group_count])] = data_dic['data{}'.format(table_no)]
                last_group = 'Group {}'.format(groups[group_count])
                group_count+=1
            # Starting new group for data whose groups are not indexed on above pages
            elif actual_groups:
                if table_no>=actual_groups[0][0] and table_no<=actual_groups[-1][0]:
                    data_dic['data{}'.format(table_no)].columns  = ['GSS','Feeder No','Affected area']
                    main_dict[data_dic['data{}'.format(table_no)].iloc[0].values[0]]= data_dic['data{}'.format(table_no)][2:]

                    last_group = data_dic['data{}'.format(table_no)].iloc[0].values[0]
            # it is concatenating continous data of last group(Note: one filter only: which is.. it should have 3 columns
            #  no way of knowing what data is on current page.)
            else:
                # setting index for this
                data_dic['data{}'.format(table_no)].columns = ['GSS','Feeder No','Affected area']
                main_dict[last_group] = pd.concat([main_dict[last_group],data_dic['data{}'.format(table_no)]])

    # Resetting index
    for group,table in main_dict.items():
        table.reset_index(drop=True)
    # Cleaning new lines ('/n') and splitting it using separator (',')
    for table in main_dict.values():
        table['Affected area'] = table['Affected area'].apply(lambda x : list(filter(None,[y.strip() for y in x.replace("\n", "").split(',')])))
        table['GSS'] = table['GSS'].apply(lambda x : x.replace('\n',''))
    # Fixing multiple rows issue in single row
    for table in main_dict.values():
        table['GSS'][table['GSS']==''] = np.NaN
        table['Feeder No'][table['Feeder No']==''] = np.NaN
        table['GSS'].fillna(method='ffill')
        table['Feeder No'].fillna(method='ffill')

    # Creating final output as json dictionary
    final_dic = {}
    for group,table in main_dict.items():
        for row in table.iterrows():
            # checking if GSS is already stored in as keys of final_dic
            if row[1][0] in final_dic.keys():
                # looping through places to save data
                for place in row[1][2]:
                    place = place.title()
                    place = re.sub(r'\srd',' Road',place,flags=re.IGNORECASE)
                    place = re.sub(r'\spl',' Road',place,flags=re.IGNORECASE)
                    place = re.sub(r'\bleco\s(areas|araes).+\.?','',place,flags=re.IGNORECASE)
                    # skipping place which is empty after cleaning the place
                    if not len(place):
                        continue
                    # checking if place is already stored or not.. as key of District
                    if place in final_dic['{}'.format(row[1][0])].keys():
                        final_dic['{}'.format(row[1][0])][place]['groups'].append(group.split()[1])
                        final_dic['{}'.format(row[1][0])][place]['feeders'].append(row[1][1])

                        # saving only unique groups and feeder No
                        final_dic['{}'.format(row[1][0])][place]['groups'] = list(set(final_dic['{}'.format(row[1][0])][place]['groups']))
                        final_dic['{}'.format(row[1][0])][place]['feeders'] = list(set(final_dic['{}'.format(row[1][0])][place]['feeders']))

                    # if place is not saved yet
                    else:
                        final_dic['{}'.format(row[1][0])][place] = {'groups':[(group.split()[1])],'feeders':[row[1][1]]}
            else:
                final_dic['{}'.format(row[1][0])] = {}
                for place in row[1][2]:
                    final_dic['{}'.format(row[1][0])][place] = {'groups':[(group.split()[1])],'feeders':[row[1][1]]}

    # Save into a file for dev
    if dev_mode:
        with open('./outputs/extracted_places.json', 'w') as outfile:
            json.dump(final_dic, outfile, indent=4)

    return final_dic



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

    # Detect dates
    #informed_days = get_dates(localDocPath)
    #logger.info("Informed days: %s" % (informed_days))

    # Extract data
    extracted_data = extract_data(localDocPath)
    json_places = extracted_data[0]
    json_schedules = extracted_data[1]

    # Print extracted places
    gss_count = len(json_places)
    area_count = 0
    for gss_name in json_places.keys():
        current_gss_areas = len(json_places[gss_name])
        area_count = area_count + current_gss_areas
    logger.info("Extracted places: %s areas in %s gss" % (area_count, gss_count))
    #logger.info(json_places)

    # Print extracted schedules
    schedules_count = len(json_schedules["schedules"])
    logger.info("Extracted schedules: %s power cuts" % (schedules_count))
    #logger.info(json_schedules)

    # Sucessful finish
    logFinish("Scraper finished successfully!")

    # Death old code

    # Extract schedules (old way)
    json_schedules = old_extract_schedules(localDocPath)
    dict_schedules = {"schedules": json.loads(json_schedules)}
    schedules_count = len(json_schedules)
    logger.info("Old schedules: %s new items" % (schedules_count))
    #logger.info(dict_schedules)


    # Pushing schedules to our API
    if dev_mode:
        # Skipping the post
        logger.info("Skipping data post to API")
        logFinish("Skipped post of %s entries" % (schedules_count))
    else:
        # Post the scraped data into our API
        api_url = 'https://hackforsrilanka-api.herokuapp.com/api/illuminati/data'
        logger.info("Post data to API at: " + api_url)
        response = requests.post(api_url, json=dict_schedules)

        # Log the response from API
        logger.info("Response code: " + str(response.status_code))
        logger.info("Response reason: " + response.reason)
        logger.info("Response content: " + str(response.content))

        if (response.status_code == 200):
            storage.save_processed(targetId)
            logFinish("Data posted successfully (%s entries" % (schedules_count))
        else:
            logger.error("Error posting data")
            logFinish("Error posting data")
