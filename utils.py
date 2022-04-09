import requests
from lxml import html
import camelot
import pandas as pd

from datetime import datetime
import pytz
import uuid
import json

# request, bypass certificate check


# get Sri Lanka local time
# sl_time = datetime.now(pytz.timezone('Asia/Colombo')).strftime('%Y-%m-%d')

class power_cut_schedule():

    def __int__(self):
        # get Sri Lanka local time
        self.sl_time = datetime.now(pytz.timezone('Asia/Colombo')).strftime('%Y-%m-%d')
        self.destination = './assets/ceb_googledoc'

    def retrieve_url(self):
        req = requests.get('https://ceb.lk', verify=False)
        webpage = html.fromstring(req.content)
        links = webpage.xpath('//a/@href')

        # look for google drive link, remove duplicates
        self.id = list(set([i for i in links if 'drive.google.com' in i]))

    def get_confirm_token(self, response):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value
        return None

    def convert_time(self, time_str):
        time_str = self.sl_time + 'T' + time_str + ':00.000Z'
        return time_str

    def save_response_content(self):
        CHUNK_SIZE = 32768

        with open(self.destination, "wb") as f:
            for chunk in self.response.iter_content(CHUNK_SIZE):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)

    def download_file_from_google_drive(self):
        URL = "https://docs.google.com/uc?export=download"

        session = requests.Session()

        response = session.get(URL, params={'id': self.id}, stream=True)
        token = self.get_confirm_token(response)

        if token:
            params = {'id': self.id, 'confirm': token}
            self.response = session.get(URL, params=params, stream=True)

        self.save_response_content(self)

    def retrieve_tables(self):
        self.file_id = self.id[0].split('/')[5]
        self.download_file_from_google_drive(self)
        self.tables = camelot.read_pdf('./assets/ceb_googledoc.pdf')

        # return self.tables

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
        json_out = self.process_tables(self).reset_index(drop=True).to_json(orient='records')
        self.dict_obj = {"schedules": json.loads(json_out)}
