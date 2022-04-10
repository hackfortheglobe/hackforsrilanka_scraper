from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess

sched = BlockingScheduler()
subprocess.run(['apt-get', 'update'])
subprocess.run(['sudo', 'dpkg', '--add-architecture', 'i386'])
subprocess.run(['apt-get','install','libgl1', '-y'])
@sched.scheduled_job('interval', minutes=1)
def timed_job():
    
    subprocess.run(["python", "main.py"])

sched.start()