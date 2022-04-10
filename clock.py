from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess

sched = BlockingScheduler()
subprocess.run(['yum update'])
subprocess.run(['yum','install','ffmpeg libsm6 libxext6  -y'])
@sched.scheduled_job('interval', minutes=1)
def timed_job():
    
    subprocess.run(["python", "main.py"])

sched.start()