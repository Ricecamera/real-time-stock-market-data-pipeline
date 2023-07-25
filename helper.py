import pytz
from datetime import datetime, timedelta

rn_file = open('running_number.txt', 'r+')
eastern = pytz.timezone('US/Eastern')

def get_last_hr_time():
  current_datetime = datetime.now(eastern)

  # Calculate 10 minutes ago
  ten_minutes_ago = datetime.combine(current_datetime.date(), current_datetime.time()) - timedelta(hours=1)
  return ten_minutes_ago

def is_market_open():
  current_time = datetime.now(eastern).time()
  market_open_time = datetime.strptime('09:30:00', '%H:%M:%S').time()
  market_close_time = datetime.strptime('16:00:00', '%H:%M:%S').time()
  return market_open_time <= current_time <= market_close_time

def get_running_number():
  rn_file.seek(0)
  text = rn_file.read().strip()
  _, i = text.split('=')
  return int(i)

def update_running_number(i: int):
  rn_file.seek(0)
  rn_file.write(f'running_number={i}\n')

def close():
  rn_file.close()