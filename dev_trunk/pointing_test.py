#!/usr/bin/env python3

__author__ = 'Devansh Agarwal'
__email__  = 'da0017@mix.wvu.edu'

# this script dumps data from redis to a json every second for 5 minutes

import redis
from datetime import datetime, timedelta
import time
import numpy as np
#from apscheduler.schedulers.background import BackgroundScheduler
import json

r = redis.StrictRedis(
    host='serendip6',
    port=6379)

# we want these keys
names=['MJD','UTC','IFV1TNCI', 'MAJOR', 'MINOR', 'RA_DRV', 'DEC_DRV']
pipe =  r.pipeline()

print(r.keys())

for key in r.keys():
    try:
        print(key, r.hgetall(key))
    except:
        pass

def tick():
    for name in names:
        pipe.hmget(name,'VALUE')
    value=pipe.execute()
    # append in list
    print(list(map(lambda x: x[0].decode(),value)))

if __name__ == '__main__':
    tick()
    #scheduler = BackgroundScheduler()
    #thelist=[]
    ## dump every second
    #scheduler.add_job(tick, 'interval', seconds=1,start_date=datetime.now(),end_date=datetime.now()+timedelta(seconds=10),args=[thelist])
    #scheduler.start()

    #try:
    #    # This is here to simulate application activity (which keeps the main thread alive).
    #    while True:
    #        time.sleep(1) # do this for 5 min then break
    #        break
    #except (KeyboardInterrupt, SystemExit):
    #    # Not strictly necessary if daemonic mode is enabled but should be done if possible
    #    scheduler.shutdown()
    #data_dict={names[0]:[float(element[0]) for element in thelist],\
    #        names[1]:[element[1] for element in thelist],\
    #        names[2]:[element[2] for element in thelist],\
    #        names[3]:[element[3] for element in thelist],\
    #        names[4]:[element[4] for element in thelist],\
    #        names[5]:[float(element[5]) for element in thelist],\
    #        names[6]:[float(element[6]) for element in thelist]}
    #with open("file.json",'w') as f:
    #    json.dump(data_dict,f,indent=4)