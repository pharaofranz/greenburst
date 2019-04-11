#!/usr/bin/env python3


import matplotlib
matplotlib.use('Agg')
from subprocess import PIPE, Popen
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import os
import logging
from multiprocessing import Process
import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError
import pylab as plt
import glob
import pika
import pysigproc
from influx_2df import mjd2influx, extend_df

__author__='Devansh Agarwal'
__email__ = 'da0017@mix.wvu.edu'

logger = logging.getLogger()
format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=format)
logging.getLogger('pika').setLevel(logging.INFO)

def stage_initer(values):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='stage02_queue', durable=True)

    def callback(ch, method, properties, body):
        values.files=body.decode() +  '/*cand'
        logging.info(f'got it {values.files}')
        begin_main(values)
        ch.basic_ack(delivery_tag = method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(on_message_callback=callback, queue='stage02_queue')

    channel.start_consuming()


def match_cands(cand_df, influx_df):
    return _new_cand_df

def begin_main(values):
    cand_lists=[]
    for files in glob.glob(values.files):
        try:
            cand_lists.append(pd.read_csv(files, header=None,\
                    comment='#',delim_whitespace=True,\
                    names=['snr','sample','tcand','width','dm_trial','dm',\
                    'members','start_sample','end_sample']))
        except EmptyDataError:
            pass
    
    if len(cand_lists) != 0:
        cand_df = pd.concat(cand_lists,ignore_index=True)
        mask_thresholds = (cand_df['snr']>= values.snr) & (cand_df['members']>= values.members) & \
                (cand_df['dm'] > values.dm) & (cand_df['width'] <= values.width)

        base_work_dir = '/ldata/trunk'
        folder=files.split('/')[-2]


        if len(cand_df[mask_thresholds]) > 0:
            fil_file = glob.glob(f'{base_work_dir}/{folder}/*.fil')[0]
            fil_obj = pysigproc.SigprocFile(fp=fil_file)
            mjd = fil_obj.tstart
            influx_df = mjd2influx(mjd)
            cand_df.loc[:,'cand_mjd'] = mjd + (cand_df['tcand']/(60*60*24))
            extended_df  = extend_df(influx_df, cand_df[mask_thresholds])
            mask = extended_df['cand_valid'] == 1
            cand_df_masked = extended_df[mask]
        else: 
            mask = mask_thresholds
            cand_df_masked = cand_df[mask]
        
        logging.info(f'Got {len(cand_df)} cands pre-filtering')
        logging.info(f'Got {len(cand_df_masked)} cands post-filtering')        
        if len(cand_df) != 0:
            plt.title(folder)
            ax=cand_df.plot('tcand','dm',kind='scatter',\
                    marker='.',c='none', edgecolors='k',s=cand_df.snr/1.0,alpha=0.5)
            if len(cand_df_masked) !=0:
                kill_mask = glob.glob(f'{base_work_dir}/{folder}/*.flag')
                cand_df_masked.loc[:,'fil_file']= fil_file
                cand_df_masked.loc[:,'kill_mask']= kill_mask[0]
                cand_df_masked.plot('tcand','dm',kind='scatter',\
                        marker='D',c='none', edgecolors='r',s=cand_df_masked.snr/1.0,ax=ax)

                cand_df_masked.to_csv(f'{base_work_dir}/{folder}/{folder}.csv',index=False,
                        header=True,columns=['fil_file','snr','tcand','dm','width','kill_mask',
                                              'cand_mjd','ATAZIND','ATELIND','AZCORR','ELCORR','RA_deg',
                                              'DEC_deg','SCPROJID','WEBCNTRL','IFV1TNCI','ATRXOCTA',
                                              'cand_gl','cand_gb','cand_ne2001','cand_ymw16','cand_valid'])
            plt.xlabel('Time (s)')
            plt.ylabel('DM (pc/cc)')
            plt.savefig(f'{base_work_dir}/{folder}/{folder}.png', bbox_inches='tight')
            plt.close()


    else:
        logging.info('No cands here!')


    return None

if __name__=='__main__':

    parser=ArgumentParser(description='Stage 2: Sort Cands', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.add_argument('-d', '--daemon', dest='daemon', action='store_false', help='Run with AMQP')
    parser.add_argument('-s', '--snr', type=int, help='sigma over which values are tagged as RFI', default=9.5)
    parser.add_argument('-w', '--width', type=int, help = 'log 2 width of the candidates', default=7)
    parser.add_argument('-D', '--dm', type=float, help = 'minimum DM to look out for', default=30)
    parser.add_argument('-m', '--members', type=int, help='minimum number of members in the cluster', default=5)
    parser.add_argument('-f', '--files', type=str, help='cand files')
    parser.set_defaults(verbose=False)
    parser.set_defaults(daemon=True)
    values = parser.parse_args()
    
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if values.daemon:
        logging.info('Running in daemon mode')
        stage_initer(values)
    else:
        begin_main(values)
