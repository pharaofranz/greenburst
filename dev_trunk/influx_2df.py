#!/usr/bin/env python3

import yaml
import pandas as pd
from astropy.time import Time
from influxdb import DataFrameClient
import logging
import numpy as np
import subprocess
from scipy.interpolate import UnivariateSpline
import logging

def mjd2influx(mjd):
    with open("config/conf.yaml", 'r') as stream:
        data_loaded = yaml.load(stream)
    login_detail = data_loaded['influxdb']
    client = DataFrameClient(host=login_detail['host'],
                             port=login_detail['port'],
                             username=login_detail['uname'],
                             password=login_detail['passw'],
                             database=login_detail['db'])

    time = Time(mjd, format = 'mjd')

    query = f"SELECT * from telescope WHERE time >='{time.iso}' AND time <= '{time.iso}' + 9m"
    logging.info(f'Querying MJD: {mjd} UTC_start:{time.iso}')

    ret = client.query(query)
    try:
        return ret['telescope']
    except KeyError:
        return None

def interpolate_value(mjd, df, key):
    spl = UnivariateSpline(df['MJD'], y=df[key])
    print(spl(mjd))
    return spl(mjd)
    
def get_both_dms(gl, gb):
    print(gl, gb)
    ne2001_cmd = f'NE2001 {gl} {gb} 25 -1'
    ymw16_cmd = f'ymw16 Gal {gl} {gb} 25000 2'
    ne2001_output = subprocess.run(ne2001_cmd.split(), stdout=subprocess.PIPE).stdout
    ymw16_output = subprocess.run(ymw16_cmd.split(), stdout=subprocess.PIPE).stdout
    nout=float(ne2001_output.split()[0])
    yout=float(ymw16_output.split(b'DM:')[1].split()[0])
    return nout, yout
"""
Index(['ATAZIND', 'ATELIND', 'ATMFBS', 'ATMTLS', 'ATRXOCTA', 'AZCORR', 'AZMJD',
       'DATA_VALID', 'DEC_deg', 'ELCORR', 'ELMJD', 'GB_deg', 'GL_deg',
       'IFV1TNCI', 'MJD', 'RA_deg', 'SCPROJID', 'UTC', 'WEBCNTRL', 'tags'],
      dtype='object')
"""
def extend_df(influx_df, cand_df, tsec = 3):
    
    cand_df.loc[:,'ATAZIND'] = None
    cand_df.loc[:,'ATELIND'] = None
    cand_df.loc[:,'AZCORR'] = None
    cand_df.loc[:,'ELCORR'] = None
    cand_df.loc[:,'RA_deg'] = None
    cand_df.loc[:,'DEC_deg'] = None
    cand_df.loc[:,'SCPROJID'] = None
    cand_df.loc[:,'WEBCNTRL'] = None
    cand_df.loc[:,'IFV1TNCI'] = None
    cand_df.loc[:,'ATRXOCTA'] = None
    cand_df.loc[:,'cand_gl'] = None
    cand_df.loc[:,'cand_gb'] = None
    cand_df.loc[:,'cand_ne2001'] = None
    cand_df.loc[:,'cand_ymw16'] = None
    cand_df.loc[:,'cand_valid'] = 1 
    
    if influx_df is None:
        logging.info('No influx data was found')
        return cand_df

    for index, row in cand_df.iterrows():
        mask = (row['cand_mjd'] - (tsec/(60*60*24)) <  influx_df['MJD']) | (influx_df['MJD'] < row['cand_mjd'] + (tsec/(60*60*24)))
        if influx_df['DATA_VALID'][mask].sum() != len(influx_df[mask]):
            cand_df.loc[index,'cand_valid'] = 0
        else:
            cand_df.loc[index,'cand_gl'] = interpolate_value(row['cand_mjd'], influx_df[mask], 'GL_deg') 
            cand_df.loc[index,'cand_gb'] = interpolate_value(row['cand_mjd'], influx_df[mask], 'GB_deg')
            ne2001_dm, ymw16_dm = get_both_dms(cand_df.loc[index,'cand_gl'], cand_df.loc[index,'cand_gb'])
            cand_df.loc[index,'cand_ne2001'] = ne2001_dm
            cand_df.loc[index,'cand_ymw16'] = ymw16_dm
            if row['dm'] >= min(cand_df.loc[index,'cand_ne2001'], cand_df.loc[index,'cand_ymw16']):
                cand_df.loc[index,'cand_valid'] = 1
            else:
                cand_df.loc[index,'cand_valid'] = 0
            cand_df.loc[index,'ATAZIND'] = interpolate_value(row['cand_mjd'], influx_df[mask], 'ATAZIND')
            cand_df.loc[index,'ATELIND'] = interpolate_value(row['cand_mjd'], influx_df[mask], 'ATELIND')
            cand_df.loc[index,'AZCORR'] = interpolate_value(row['cand_mjd'], influx_df[mask], 'AZCORR')
            cand_df.loc[index,'ELCORR'] = interpolate_value(row['cand_mjd'], influx_df[mask], 'ELCORR')
            cand_df.loc[index,'RA_deg'] = interpolate_value(row['cand_mjd'], influx_df[mask], 'RA_deg')
            cand_df.loc[index,'DEC_deg'] = interpolate_value(row['cand_mjd'], influx_df[mask], 'DEC_deg')
            cand_df.loc[index,'SCPROJID'] = set(influx_df['SCPROJID'][mask])
            cand_df.loc[index,'WEBCNTRL'] = 1
            cand_df.loc[index,'IFV1TNCI'] = set(influx_df['IFV1TNCI'][mask])
            cand_df.loc[index,'ATRXOCTA'] = set(influx_df['ATRXOCTA'][mask])
    return cand_df

if __name__ == "__main__":
    logging_format = '%(asctime)s - %(funcName)s -%(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    random_dict = {}
    MJD = 58583.85
    random_dict['tcand'] = np.random.uniform(0,490,5)
    random_dict['dm'] = np.random.uniform(0,4900,5)
    cand_df = pd.DataFrame(random_dict)
    cand_df.loc[:,'cand_mjd'] = MJD + (cand_df['tcand']/(60*60*24))
    influx_df = mjd2influx(MJD)
    print(extend_df(influx_df, cand_df))
