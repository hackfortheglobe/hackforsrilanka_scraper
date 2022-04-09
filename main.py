import requests
from lxml import html
import requests
import camelot
import pandas as pd

# request, bypass certificate check
req = requests.get('https://ceb.lk', verify=False)
webpage = html.fromstring(req.content)
links = webpage.xpath('//a/@href')
# look for google drive link
gd_link = [i for i in links if 'drive.google.com' in i]

# remove duplicates
gd_link = list(set(gd_link))

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

if __name__ == "__main__":
    file_id = gd_link[0].split('/')[5]
    destination = './ceb_googledoc.pdf'
    download_file_from_google_drive(file_id, destination)


def process_tables(tables):
    dff = pd.DataFrame()

    for ii in range(2):

        df = tables[ii].df
        df = pd.DataFrame(data=df.iloc[1:].values, columns=df.iloc[0])
        df['start_time'] = df['Period'].apply(lambda x: x.split(' ')[0]).str.replace('–', '')
        df['end_time'] = df['Period'].apply(lambda x: x.split(' ')[-1]).str.replace('–', '')

        for jj in range(len(df)):
            for ele in df['Schedule Group'][jj].replace(' ', '').split(','):
                dff = dff.append(pd.DataFrame(data={'Group': ele, 'start_time': [df.iloc[jj]['start_time']],
                                                    'end_time': [df.iloc[jj]['end_time']]}))

    return dff

tables = camelot.read_pdf('ceb_googledoc.pdf', pages = 'all')
# convert to json format {"Group":..., "start_time":..., "end_time":...}
json_out = process_tables(tables).reset_index(drop=True).to_json()