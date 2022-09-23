#!/usr/bin/env python3


#import matplotlib
#matplotlib.use('Agg')
#import threading
import subprocess
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import os
from subprocess import PIPE, Popen
import logging
from multiprocessing import Process
import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError
import pylab as plt
import glob
import pika
from pika_send import send2Q
import pysigproc
import sys
#from influx_2df import mjd2influx, extend_df
from gpu_client import send2gpuQ
from dm_utils import get_dm, get_src, get_nchan
from pika.exceptions import *
from slack_send import send_msg_2_slack

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
        message = body.decode()
        values.files = message.split()[0].strip() +  '/*cand'
        logging.info(f'got it {values.files}')
        try:
            values.flag = message.split()[1].strip()
            logging.info(f'got flag file {values.flag}')
        except:
            values.flag = None
        ch.basic_ack(delivery_tag = method.delivery_tag)
        begin_main(values)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(on_message_callback=callback, queue='stage02_queue')

    try:
        channel.start_consuming()
    except (StreamLostError, ConnectionResetError) as e:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

def _cmdline(command):
    """
    Function that captures output from screen
    """
    process = Popen(args=command,stdout=PIPE,shell=True)
    output=process.communicate()[0]
    logging.info(f'Processed {command}')
    return output

#def match_cands(cand_df, influx_df):
#    return _new_cand_df
#
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
#        mask_thresholds = (cand_df['snr']>= values.snr) & (cand_df['members']>= values.members) & (cand_df['tcand'] <= 504) &\
#                (cand_df['dm'] > values.dm) & (cand_df['width'] <= values.width)
        mask_thresholds = (cand_df['snr'] >= values.snr) & (cand_df['members'] >= values.members) &\
                (cand_df['dm'] > values.dm_min) & (cand_df['width'] <= values.width)

        base_work_dir = '/data1/franz/fetch'
        experiment = files.split('/')[-3]
        scan = files.split('/')[-2]
        folder = f'{experiment}/{scan}'

        # added by FK from jodsearch per mitch 11 Dec 2019
        cand_df_masked = cand_df[mask_thresholds]
        fil_file = glob.glob(f'{base_work_dir}/{experiment}/{scan}/*.fil')[0]
        station = fil_file.split('_')[1] # assumes file names like pr008b_bd_no0022...
        flag_file=f'{base_work_dir}/{station}.flag'
        if not values.flag is None:
            flag_file = values.flag
        if not os.path.exists(flag_file):
            logging.info(f'Supplied flag file {flag_file} does not exist, will use a default.')
            flag_file=f'{base_work_dir}/flag.flag'
        logging.info(f'Using {flag_file} as flag file')
        src = get_src(fil_file)
        expected_dm = get_dm(src)
        nchan = get_nchan(fil_file)
        logging.info(f'Got DM = {expected_dm} for source {src}')
        logging.info(f'Got {nchan} channels for {fil_file}.')

        logging.info(f'Got {len(cand_df)} cands pre-filtering')
        #send_msg_2_slack(f'Got {len(cand_df)} cands pre-filtering at MJD {mjd}')
        logging.info(f'Got {len(cand_df_masked)} cands post-filtering')
        #send_msg_2_slack(f'Got {len(cand_df_masked)} cands post-filtering at MJD {mjd}')
        if len(cand_df) != 0:
            #plt.title(folder)
            #ax=cand_df.plot('tcand','dm',kind='scatter',\
            #        marker='.',c='none', edgecolors='k',s=cand_df.snr/1.0,alpha=0.5)
            if len(cand_df_masked) !=0:
                kill_mask = glob.glob(f'{flag_file}')
                cand_df_masked.loc[:,'fil_file']= fil_file
                cand_df_masked.loc[:,'kill_mask']= kill_mask[0]
                cand_df_masked.loc[:,'label'] = 0
                #cand_df_masked.plot('tcand','dm',kind='scatter',\
                #        marker='D',c='none', edgecolors='r',s=cand_df_masked.snr/1.0,ax=ax)

                cand_df_masked.to_csv(f'{base_work_dir}/{experiment}/{scan}/{scan}.csv',index=False,
                                      header=False,columns=['fil_file','snr','tcand','dm','width','label','kill_mask'])
                        #header=True,columns=['fil_file','snr','tcand','dm','width','kill_mask',
                        #                      'cand_mjd','ATAZIND','ATELIND','AZCORR','ELCORR','RA_deg',
                        #                      'DEC_deg','RA_drift','DEC_drift','SCPROJID','WEBCNTRL','IFV1TNCI','ATRXOCTA',
                        #                      'cand_gl','cand_gb','cand_ne2001','cand_ymw16','cand_valid'])
            #plt.xlabel('Time (s)')
            #plt.ylabel('DM (pc/cc)')
            #plt.savefig(f'{base_work_dir}/{folder}/{folder}.png', bbox_inches='tight')
            #plt.close()
            if len(cand_df_masked) != 0:
                cmd = f'mkdir -p {base_work_dir}/{experiment}/{scan}/cands'
                subprocess.run(cmd.split(), stdout=subprocess.PIPE)
                if nchan < 256:
                    cmd = f'/home/franz/software/src/fetch/bin/candmaker.py -n 20 -c {base_work_dir}/{experiment}/{scan}/{scan}.csv -o {base_work_dir}/{experiment}/{scan}/cands/'
                    subprocess.run(cmd.split(), stdout=subprocess.PIPE)
                else:
                    send2gpuQ(f'/home/franz/software/src/fetch/bin/candmaker_gpu.py -n 1 -c {base_work_dir}/{experiment}/{scan}/{scan}.csv -o {base_work_dir}/{experiment}/{scan}/cands/')
                if station == 'tr':
                    send2gpuQ(f'predict.py -n 5 -c {base_work_dir}/{experiment}/{scan}/cands/ -m a -p 0.8')
                    send2gpuQ(f'predict.py -n 5 -c {base_work_dir}/{experiment}/{scan}/cands/ -m h -p 0.8')
                else:
                    send2gpuQ(f'predict.py -n 5 -c {base_work_dir}/{experiment}/{scan}/cands/ -m a')
                    send2gpuQ(f'predict.py -n 5 -c {base_work_dir}/{experiment}/{scan}/cands/ -m h')
                #send2gpuQ(f'predict.py -n 32 -b 32 -c {base_work_dir}/{experiment}/{scan}/cands/ -m a')

                results_a = pd.read_csv(f'{base_work_dir}/{experiment}/{scan}/cands/results_a.csv')
                results_h = pd.read_csv(f'{base_work_dir}/{experiment}/{scan}/cands/results_h.csv')
                results = results_a.merge(results_h, on='candidate', how='inner')
                results["dm"] = results["dm_x"]
                results["label"] = results[["label_x", "label_y"]].max(axis=1)
                results["probability"] = results[["probability_x", "probability_y"]].max(axis=1)
                snr = [float(cand.split('snr_')[1].split('.h5')[0]) for cand in results['candidate']]
                results['snr'] = snr
                label_mask = (results['label'] == 1)
                #expected_dm = None
                if expected_dm is not None:
                    dm_mask = (expected_dm - 5 <= results['dm']) & \
                              (results['dm'] <= expected_dm + 5) # & \
                              #(results['snr'] > 10.0)
                else:
                    dm_mask = label_mask
                #send_msg_2_slack(f'Got {label_mask.sum()} cands post-FETCH at MJD {mjd}')
                if label_mask.sum() + dm_mask.sum() >= 1:
                    ncands = label_mask.sum() + dm_mask.sum()
                    slackconfig = "config/conf.yaml"
                    if 'fake' in experiment:
                        slackconfig = "config/fakefrb.yaml"
                    send_msg_2_slack(f'{scan}, {ncands} cands', config=slackconfig)
                    files = results[label_mask | dm_mask]['candidate']
                    for h5_file in list(files):
                        send2Q("stage03_queue", h5_file)
                    #for file in files:
                    #    send2Q("stage03_queue", file)
                #else:
                    #delete_cmd = "put delete cmd here"
                    #subprocess.run(cmd.split(), stdout=subprocess.PIPE)

    else:
        logging.info('No cands here!')

    return None

if __name__=='__main__':

    parser=ArgumentParser(description='Stage 2: Sort Cands', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.add_argument('-d', '--daemon', dest='daemon', action='store_false', help='Run with AMQP')
    parser.add_argument('-s', '--snr', type=int, help='sigma over which values are tagged as RFI', default=7.0)
    parser.add_argument('-w', '--width', type=int, help = 'log 2 width of the candidates', default=7) #10
    parser.add_argument('-D', '--dm_min', type=float, help = 'minimum DM to look out for', default=10) #10
    parser.add_argument('-m', '--members', type=int, help='minimum number of members in the cluster', default=2) #2
    parser.add_argument('-f', '--files', type=str, help='cand files')
    parser.add_argument('-F', '--flag', type=str, help='Flag file')
    parser.set_defaults(verbose=False)
    parser.set_defaults(daemon=True)
    parser.set_defaults(flag=None)
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
