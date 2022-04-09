import requests
from lxml import html
import camelot
import pandas as pd

from datetime import datetime
import pytz
import json

# get Sri Lanka local time
sl_time = datetime.now(pytz.timezone('Asia/Colombo')).strftime('%Y-%m-%d')

# get url from 'ceb/lk' website
def retrieve_url():
    # get new url
    req = requests.get('https://ceb.lk', verify=False)
    webpage = html.fromstring(req.content)
    links = webpage.xpath('//a/@href')
    # look for google drive link
    url_link = list(set([i for i in links if 'drive.google.com' in i]))[0]

    return url_link

# compare with existing
def compare_url(url_link):
    # retrieve exising url
    f = open('./assets/gd_url.json')
    existing_url = json.load(f)['url']
    f.close()
    if url_link == existing_url:
        return False
    else:
        f = open('./assets/gd_url.json', 'w')
        f.write(json.dumps({'url': url_link[0]}))
        f.close()
        return True

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
def process_tables(tables):
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
                                                    'ending_period': [df.iloc[jj]['ending_period']],
                                                    }))
        return dff

# TODO implement callable Class with above functions
class power_cut_schedule:
    def __int__(self):
        # get Sri Lanka local time
        self.sl_time = datetime.now(pytz.timezone('Asia/Colombo')).strftime('%Y-%m-%d')
        self.destination = './assets/ceb_googledoc.pdf'

    def retrieve_url(self):
        # bypass certificate check
        req = requests.get('https://ceb.lk', verify=False)
        webpage = html.fromstring(req.content)
        links = webpage.xpath('//a/@href')

        # look for google drive link
        url_link = [i for i in links if 'drive.google.com' in i]
        self.id = list(set(url_link))[0].split('/')[5]

        return list(set(url_link))[0].split('/')[5]

    def convert_time(self, time_str):
        time_str = self.sl_time + 'T' + time_str + ':00.000Z'
        return time_str

    def download_file_from_google_drive(self):
        URL = "https://docs.google.com/uc?export=download"
        session = requests.Session()

        id = self.retrieve_url()

        self.response = session.get(URL, params={'id': id}, stream=True)
        token = self.get_confirm_token(self)

        if token:
            params = {'id': id, 'confirm': token}
            self.response = session.get(URL, params=params, stream=True)

        self.save_response_content(self)

    def get_confirm_token(self):
        for key, value in self.response.cookies.items():
            if key.startswith('download_warning'):
                return value
        return None
    def save_response_content(self):
        CHUNK_SIZE = 32768

        with open(self.destination, "wb") as f:
            for chunk in self.response.iter_content(CHUNK_SIZE):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
    def retrieve_tables(self):
        self.download_file_from_google_drive(self)
        self.tables = camelot.read_pdf(self.destination)
    def process_tables(self):
        dff = pd.DataFrame()

        for ii in range(2):

            df = self.tables[ii].df

            df = pd.DataFrame(data=df.iloc[1:].values, columns=df.iloc[0])
            df['starting_period'] = df['Period'].apply(lambda x: x.split(' ')[0] if ' ' in x else x.split('-')[0])
            df['ending_period'] = df['Period'].apply(lambda x: x.split(' ')[-1] if ' ' in x else x.split('-')[-1])

            df['starting_period'] = df['starting_period'].apply(lambda x: x.replace('–', '')).apply(lambda x: self.convert_time(x))
            df['ending_period'] = df['ending_period'].apply(lambda x: x.replace('–', '')).apply(lambda x: self.convert_time(x))

            for jj in range(len(df)):
                eles = df['Schedule Group'][jj].replace(' ', '').split(',')
                eles = ' '.join(eles).split()
                for ele in eles:
                    dff = dff.append(pd.DataFrame(data={'group_name': ele, 'starting_period': [df.iloc[jj]['starting_period']],
                                                        'ending_period': [df.iloc[jj]['ending_period']],
                                                        }))
        return dff
    def output_json(self):
        json_out = self.process_tables().reset_index(drop=True).to_json(orient='records')
        # self.dict_obj = {"schedules": json.loads(json_out)}
        return {"schedules": json.loads(json_out)}

    def post_json(self):
        requests.post('https://hackforsrilanka-api.herokuapp.com/api/illuminati/data', json=self.output_json())
