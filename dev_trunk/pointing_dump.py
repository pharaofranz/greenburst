#!/usr/bin/env python3

__author__ = 'Devansh Agarwal'
__email__  = 'da0017@mix.wvu.edu'

# this script dumps data from redis to a json every second for 5 minutes

import redis
from datetime import datetime, timedelta
import time
import sys
import numpy as np
from apscheduler.schedulers.background import BackgroundScheduler
import json
import os

r = redis.StrictRedis(
    host='serendip6',
    port=6379)

# we want these keys
names=['MJD','UTC','IFV1TNCI', 'MAJOR', 'MINOR', 'RA_DRV', 'DEC_DRV']
pipe =  r.pipeline()


def tick(thelist):
    for name in names:
        pipe.hmget(name,'VALUE')
    value=pipe.execute()
    # append in list
    lister=list(map(lambda x: x[0].decode(),value))
    lister[1]=datetime.utcnow().strftime("%Y-%m-%d ")+lister[1]
    thelist.append(lister)

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    thelist=[]
    # dump every second
    scheduler.add_job(tick, 'interval', seconds=1,start_date=datetime.now(),end_date=datetime.now()+timedelta(seconds=337),args=[thelist])
    scheduler.start()

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(337) # do this for 5 min then break
            break
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
    data_dict={names[0]:[float(element[0]) for element in thelist],\
            names[1]:[element[1] for element in thelist],\
            names[2]:[element[2] for element in thelist],\
            names[3]:[element[3] for element in thelist],\
            names[4]:[element[4] for element in thelist],\
            names[5]:[float(element[5]) for element in thelist],\
            names[6]:[float(element[6]) for element in thelist]}
    outname=str(sys.argv[1])+'.json'
    with open(outname,'w') as f:
        json.dump(data_dict,f,indent=4)
