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
import string
import numpy as np

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
def extract_locations(pdf_loc):
    # reading the pdf file
    tables = camelot.read_pdf(pdf_loc,pages='all')
    # dictionary to store all tables in pandas dataframe with keys assigned as data0,data1 and so on..
    # for all tables found by camelot
    data_dic = {}
    # getting all the tables from pdf file
    for no in range(0,len(tables)):
        data_dic['data{}'.format(no)] = tables[no].df
    # It checks group info in each table dataFrame by looping through it 'x' is column no, 'no'  is table no

    all_groups = []
    actual_groups = []
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
        for x in data_dic['data{}'.format(table_no)].iloc[:,all_groups[1][1]].values:
            for letter in string.ascii_uppercase:
                if letter in x:
                    groups.append(letter)
    groups = [x for x in groups if x in groups]
    groups = set(groups)
    groups = sorted(groups)

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




    # Resetting index,converting into dic and saving it to the file.
    for group,table in main_dict.items():
        table.reset_index(drop=True)
    # cleaning new lines and splitting it using (',')
    for table in main_dict.values():
        table['Affected area'] = table['Affected area'].apply(lambda x : list(filter(None,[y.strip() for y in x.replace("\n", "").split(',')])))
        table['GSS'] = table['GSS'].apply(lambda x : x.replace('\n',''))
    # Fixing multiple rows issue in single row
    for table in main_dict.values():
        table['GSS'][table['GSS']==''] = np.NaN
        table['Feeder No'][table['Feeder No']==''] = np.NaN
        table['GSS'].fillna(method='ffill')
        table['Feeder No'].fillna(method='ffill')
    final_dic = {}
    for group,table in main_dict.items():
        for row in table.iterrows():
            # checking if GSS is already stored in as keys of final_dic
            if row[1][0] in final_dic.keys():
                # looping through places to save data
                for place in row[1][2]:
                    # checking if place is already stored or not.. as key of District
                    if place in final_dic['{}'.format(row[1][0])].keys():
                        final_dic['{}'.format(row[1][0])][place]['Group'].append(group.split()[1])
                        final_dic['{}'.format(row[1][0])][place]['Feeder No'].append(row[1][1])

                        # saving only unique groups and feeder No
                        final_dic['{}'.format(row[1][0])][place]['Group'] = list(set(final_dic['{}'.format(row[1][0])][place]['Group']))
                        final_dic['{}'.format(row[1][0])][place]['Feeder No'] = list(set(final_dic['{}'.format(row[1][0])][place]['Feeder No']))

                    # if place is not saved yet
                    else:
                        final_dic['{}'.format(row[1][0])][place] = {'Group':[(group.split()[1])],'Feeder No':[row[1][1]]}


            else:
                final_dic['{}'.format(row[1][0])] = {}
                for place in row[1][2]:
                    final_dic['{}'.format(row[1][0])][place] = {'Group':[(group.split()[1])],'Feeder No':[row[1][1]]}
    return final_dic



















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
