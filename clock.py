from apscheduler.schedulers.blocking import BlockingScheduler
from utils import *

sched = BlockingScheduler()

@sched.scheduled_job('interval', minutes=10)
def timed_job():

    url_link = retrieve_url()

    if compare_url(url_link):

        file_id = url_link[0].split('/')[5]
        destination = './assets/ceb_googledoc.pdf'
        download_file_from_google_drive(file_id, destination)
        tables = camelot.read_pdf(destination)
        json_out = process_tables(tables).reset_index(drop=True).to_json(orient='records')
        dict_obj = {"schedules": json.loads(json_out)}
        requests.post('https://hackforsrilanka-api.herokuapp.com/api/illuminati/data', json=dict_obj)

    else:
        pass

sched.start()