#!/usr/bin/env python
import matplotlib
matplotlib.use('Agg')
from subprocess import PIPE, Popen
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging
import pysigproc
from multiprocessing import Process
import glob
import os
import numpy as np
import pika
from pika_send import send2Q
#from scipy.signal import savgol_filter
from gpu_client import send2gpuQ
#from influx_2df import mjd2influx
import sys
import threading
from dm_utils import get_dm, get_src
from pika.exceptions import * #StreamLostError, ConnectionResetError
#from dump_all import tel_df_to_es
#from elasticsearch import Elasticsearch

logger = logging.getLogger()
format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=format)
logging.getLogger('pika').setLevel(logging.INFO)

__author__='Devansh Agarwal'
__email__ = 'da0017@mix.wvu.edu'

def stage_initer(values):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    
    channel.queue_declare(queue='stage01_queue', durable=True)
    
    def callback(ch, method, properties, body):
        #values.file=body.decode()
        message=body.decode()
        logging.info(f'got it {message}')
        #logging.info(f'got it {values.file}')
        values.file = message.split()[0].strip()
        try:
            values.flag = message.split()[1].strip()
        except:
            pass
        ch.basic_ack(delivery_tag = method.delivery_tag)
        begin_main(values)
        logging.info('Done')
       # ch.basic_ack(delivery_tag = method.delivery_tag)
        logging.info("Ack'ed")
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(on_message_callback=callback,
                          queue='stage01_queue')
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


def begin_main(values):
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG, format=format)
    else:
        logging.basicConfig(level=logging.INFO, format=format)

    try:
        logging.info(f'{values.file} {values.flag}')
        filterbank=glob.glob(values.file)[0]
        logging.info(f'filterbank = {filterbank}')
        filterbank_name=filterbank.split('/')[-1].split('.')[0]
        experiment=filterbank_name.split('_')[0]
        out_dir_base='/data1/franz/fetch/'
        out_dir=f'{out_dir_base}/{experiment}/{filterbank_name}/'
        _cmdline('mkdir -p {}'.format(out_dir))
        _cmdline(f'ln -s {filterbank} {out_dir}/')
        new_fil_path=f'{out_dir}{filterbank_name}.fil'
        station=filterbank_name.split('_')[1]
        src = get_src(new_fil_path)
        expected_dm = get_dm(src)
        logging.info(f'Got DM = {expected_dm} for source {src}')
        min_dm = 10
        max_dm = 1500
        if not expected_dm == None:
            min_dm = int(max(10, expected_dm - 50))
            max_dm = int(expected_dm + 50)
        flag_file=f'{out_dir_base}/{station}.flag'
        if not values.flag == None:
            flag_file = values.flag
        if not os.path.exists(flag_file):
            logging.info(f'Supplied flag file {flag_file} does not exist, will use a default.')
            flag_file=f'{out_dir_base}/flag.flag'
        logging.info(f'Using {flag_file} as flag file')
        with open(flag_file, 'r') as f:
            flags = f.readlines()[0].strip().split(' ')
            if '#' in flags[0]:
                flags = flags[1:]
        heimdall_flags = ['-zap_chans {0} {1}'.format(flag.split('-')[0], flag.split('-')[1]) \
                          for flag in flags]
        # 524288 262144
        #-baseline_length 1
        heimdall_command='heimdall -nsamps_gulp 262144 -dm {0} {1}'.format(min_dm, max_dm) +\
            ' -boxcar_max 1024 -cand_sep_dm_trial 200 -cand_sep_time 128' \
            ' -cand_sep_filter 3 -dm_tol 1.05 -detect_thresh 7' \
            ' -output_dir {}'.format(out_dir) + ' -f {}'.format(new_fil_path) +\
            ' {0}'.format(' '.join(heimdall_flags))
        logging.info(f'Running {heimdall_command}')

        p1 = Process(target = send2gpuQ,args=[heimdall_command])
        p1.start()
        p1.join()
        send2Q('stage02_queue', f'{out_dir} {flag_file}')
    except IndexError:
        pass
    return None

if __name__ == '__main__':
    parser=ArgumentParser(description='Stage 1: Get RFI Flags, run heimdall', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.add_argument('-d', '--daemon', dest='daemon', action='store_false', help='Run with AMQP')
    #parser.add_argument('-n', '--nchans', type=int, help='no. of chans to calc. median over', default=64)
    #parser.add_argument('-s', '--sigma', type=int, help='sigma over which values are tagged as RFI', default=3)
    parser.add_argument('-f', '--file', type=str, help='Filterbank file')
    parser.add_argument('-F', '--flag', type=str, help='Flag file')
    parser.set_defaults(verbose=False)
    parser.set_defaults(daemon=True)
    parser.set_defaults(flag=None)
    values = parser.parse_args()

    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG, format=format)
    else:
        logging.basicConfig(level=logging.INFO, format=format)

    if values.daemon:
        logging.info('Running in daemon mode')
        stage_initer(values)
    else:
        begin_main(values)
