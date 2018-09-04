#!/usr/bin/env python3

__author__ = 'Devansh Agarwal'
__email__  = 'da0017@mix.wvu.edu'


import redis
import json
import pandas as pd
import collections
import numpy as np
from astropy.coordinates import EarthLocation, AltAz, SkyCoord
import astropy.units as u
from astropy.time import Time, TimeMJD
from influxdb import InfluxDBClient, DataFrameClient
import time
from apscheduler.schedulers.background import BackgroundScheduler
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# needed for the live RA DEC values
location=EarthLocation.of_site('GBT')

# redis connection
r = redis.StrictRedis(
    host='serendip6',
    port=6379)

# push stuff to influxdb
client=DataFrameClient(host='127.0.0.1', port=8086, username='telegraf', password='superpa$$word', database='telegraf')

# this will be used for the data validity
def flag_maker(key,val):
    if key == 'SCPROJID' and val != "JMAINT":
        return 1

    elif key == 'ATMTLS' and val == 'TURRET_LOCK_LOCKED':
        return 1

    elif key == 'ATMFBS' and val == 'BOOM_RETRACTED':
        return 1

    else:
        return 0

def data_valid(dict):
    maintainance_flag = flag_maker('SCPROJID', dict['SCPROJID'])
    turret_lock_flag  = flag_maker('ATMTLS', dict['ATMTLS'])
    boom_flag = flag_maker('ATMFBS', dict['ATMFBS'])
    obs_permission = dict['WEBCNTRL']
    return maintainance_flag and turret_lock_flag and boom_flag and obs_permission


# Ron's corrections, check GBT memo for commensal observations
def pointing_corr(turret_angle,tel_az,tel_el):
    Az = (-25.18/60)*np.sin(turret_angle*np.pi/180.0) + tel_az
    El = (-23.30/60)*(1 - np.cos(turret_angle*np.pi/180)) + tel_el
    return El, Az


# we want these keys
'''
MJD:        MJD
UTC:        UTC
IFV1TNCI:   Rcvr in use?
SCPROJID:   Project ID
ATMTLS:     Turret lock
ATMFBS:     Boom status
ATRXOCTA:   Turret Angle for L band.
ATAZIND:    Az.
ATELIND:    El.
WEBCNTRL:   Has observer allowed us to get a copy?
'''

# data types for key and values
f=float
integer=int
s=str
val_type=[f,s,s,f,s,s,f,f,s,integer]

# keys
names=['MJD','UTC','IFV1TNCI', 'ATRXOCTA', 'SCPROJID', 'ATMTLS', 'ATAZIND', 'ATELIND','ATMFBS','WEBCNTRL']

def get_pipe():
    pipe =  r.pipeline()
    
    # redis pipeline
    for name in names:
        if name == 'WEBCNTRL':
            pipe.get('WEBCNTRL')
        else:
            pipe.hmget(name,'VALUE')
    return pipe

def main():
    pipe=get_pipe()
    value=pipe.execute()
    
    # append in list
    #value=list(map(lambda x: x[0].decode(),value))
    
    telescope_status= collections.OrderedDict()
    telescope_status['tags']='gbt'
    # if float make float
    for i,val in enumerate(value):
        if val_type[i] == f:
            telescope_status[f'{names[i]}'] = float(val[0].decode())
        elif val_type[i] == integer:
            telescope_status[f'{names[i]}'] = int(val)
        else:
            telescope_status[f'{names[i]}'] = str(val[0].decode())
    # the Alt Az values are corrected for the turret location
    alt,az=pointing_corr(telescope_status['ATRXOCTA'],telescope_status['ATAZIND'],telescope_status['ATELIND'])
    # Use the redis time to get the RA DEC from the alt, az values
    time=Time(telescope_status['MJD'],format='mjd')
    obj=AltAz(obstime=time,location=EarthLocation.of_site('GBT'),az=az*u.deg, alt=alt*u.deg)
    c=SkyCoord(obj,equinox='J2000.0').transform_to('fk5')
    
    #add few more values to the dict
    telescope_status['DATA_VALID'] = data_valid(telescope_status)
    telescope_status['RA_deg']=c.ra.deg
    telescope_status['DEC_deg']=c.dec.deg
    telescope_status['GL_deg']=c.galactic.l.deg
    telescope_status['GB_deg']=c.galactic.b.deg
    telescope_status['ELCORR']=alt
    telescope_status['AZCORR']=az
    df=pd.DataFrame(telescope_status,index=[pd.to_datetime(Time(telescope_status['MJD'],format='mjd').iso)])
    val=client.write_points(df,measurement='telescope')
    return val

    # output to telegraf
    #out=f'telescope,tag="GBT" backend="greenburst"'
    #for key, val in telescope_status.items():
    #    if isinstance(val, float) or isinstance(val, int):
    #        out+=f',{key}={val}'
    #    else:
    #        out+=f',{key}="{val}"'
    #return out,telescope_status

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    sleep_time=int(2*3600)
    scheduler.add_job(main, 'interval', seconds=1,start_date=datetime.now(),end_date=datetime.now()+timedelta(seconds=sleep_time))
    scheduler.start()

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(sleep_time) # do this for 5 min then break
            break
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
