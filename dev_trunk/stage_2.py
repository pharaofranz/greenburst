#!/usr/bin/env python3


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
    channel.basic_consume(callback,
                          queue='stage02_queue')

    channel.start_consuming()


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
    
    if cand_lists:
        cand_df = pd.concat(cand_lists,ignore_index=True)
        mask = (cand_df['snr']>= values.snr) & (cand_df['members']>= values.members) & \
                (cand_df['dm'] > values.dm) & (cand_df['width'] <= values.width)
        cand_df = cand_df[mask]
        if len(cand_df) != 0:
            cand_df.plot('tcand','dm',kind='scatter',\
                    marker='o',facecolors='none', edgecolors='r',s=cand_df.snr/2)
            plt.xlabel('Time (s)')
            plt.ylabel('DM (pc/cc)')
            folder=files.split('/')[-2]
            plt.title(folder)
            base_work_dir = '/ldata/trunk/'
            plt.savefig(f'{base_work_dir}/{folder}/{folder}.png', bbox_inches='tight')
            #plt.savefig(f'~/Dropbox/plots/{folder}.png', bbox_inches='tight')
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
    parser.add_argument('-D', '--dm', type=float, help = 'minimum DM to look out for', default=0)
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
