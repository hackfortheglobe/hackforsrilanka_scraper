from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess

sched = BlockingScheduler()

@sched.scheduled_job('interval', minutes=10)
def timed_job():

    subprocess.run(["python", "main.py"])

    # if compare_url(url_link):
    #     json_out = process_tables(tables).reset_index(drop=True).to_json(orient='records')
    #     dict_obj = {"schedules": json.loads(json_out)}
    #     requests.post('https://hackforsrilanka-api.herokuapp.com/api/illuminati/data', json=self.output_json())

sched.start()