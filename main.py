import requests
from lxml import html
import requests
import camelot
import pandas as pd

from datetime import datetime
import pytz
import uuid

# request, bypass certificate check
req = requests.get('https://ceb.lk', verify=False)
webpage = html.fromstring(req.content)
links = webpage.xpath('//a/@href')

# get Sri Lanka local time
sl_time = datetime.now(pytz.timezone('Asia/Colombo')).strftime('%Y-%m-%d')

# look for google drive link
gd_link = [i for i in links if 'drive.google.com' in i]
# remove duplicates
gd_link = list(set(gd_link))

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
                                                    'unique_id': str(uuid.uuid1())}))
        return dff

if __name__ == "__main__":
    file_id = gd_link[0].split('/')[5]
    destination = './assets/ceb_googledoc.pdf'
    download_file_from_google_drive(file_id, destination)
    tables = camelot.read_pdf('./assets/ceb_googledoc.pdf')
    # convert to json format {"group":..., "start_time":..., "end_time":...}
    json_out = process_tables(tables).reset_index(drop=True).to_json(orient='records')

    # print(json_out)




