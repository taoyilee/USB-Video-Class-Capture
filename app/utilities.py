import datetime
import time


def current_timestamp():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H%M%S'), ts
