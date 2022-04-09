from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess

sched = BlockingScheduler()

@sched.scheduled_job('interval', minutes=10)
def timed_job():

    subprocess.run(["python", "main.py"])

sched.start()