import pytz
from datetime import datetime

def is_market_open():
    eastern = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern).time()
    market_open_time = datetime.strptime('09:30:00', '%H:%M:%S').time()
    market_close_time = datetime.strptime('16:00:00', '%H:%M:%S').time()
    return market_open_time <= current_time <= market_close_time