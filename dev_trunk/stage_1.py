#!/usr/bin/env python3.6

from subprocess import PIPE, Popen
import filutils
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging
from multiprocessing import Process
import glob
import numpy as np
import pika


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
        values.file=body.decode()
        logging.info(f'got it {values.file}')
        begin_main(values)
        ch.basic_ack(delivery_tag = method.delivery_tag)
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback,
                          queue='stage01_queue')
    
    channel.start_consuming()

def mask_finder(data,chan_nos,nchan_avg,sigma):
    """
    Finds the mask for the bandpass
    """
    total_mask=np.empty(data.shape[0],dtype=bool)*False
        
    for ii in range(int(data.shape[0]/nchan_avg)):
        median=np.median(data[ii*nchan_avg:(ii+1)*nchan_avg-1])
        total_mask[ii*nchan_avg:(ii+1)*nchan_avg-1]=data[ii*nchan_avg:(ii+1)*nchan_avg-1]>=(sigma*median)
    return total_mask

def _cmdline(command):	
    """
    Function that captures output from screen
    """
    process = Popen(args=command,stdout=PIPE,shell=True)
    output=process.communicate()[0]
    logging.info(f'Processed {command}')
    return output

def write_and_plot(mask,chan_nos,freqs,bandpass,outdir):
    """
    Writes and plots the bandpass
    """
    bad_chans=outdir+'bad_chans.flag'
    bp_plot=outdir+'bandpass.png'
    logging.info('Flagged %d channels',mask.sum())
    with open(bad_chans,'w') as f:
        np.savetxt(f,chan_nos[mask],fmt='%d',delimiter=' ', newline=' ')

    import matplotlib.pyplot as plt
    fig = plt.figure()
    ax11 = fig.add_subplot(111)
    ax11.plot(chan_nos,bandpass,'k-',label="Bandpass")
    ax11.plot(chan_nos[mask],bandpass[mask],'ro',label="Flaged Channels")
    ax11.set_xlabel("Chan. no.")
    ax11.set_ylabel("Arb. Units")
    ax21 = ax11.twiny()
    ax21.plot(freqs[mask],bandpass[mask],'ro')
    ax21.invert_xaxis()
    ax21.set_xlabel("Frequency (MHz)")
    ax11.legend()
    plt.savefig(bp_plot,bbox_inches='tight')



def begin_main(values):
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG, format=format)
    else:
        logging.basicConfig(level=logging.INFO, format=format)

    filterbank=glob.glob(values.file)[0]
    
    freqs, bandpass=filutils.bandpass(filterbank)
    chan_nos=np.arange(0,bandpass.shape[0])
    mask=mask_finder(bandpass,chan_nos,values.nchans,values.sigma)
    bad_chans=chan_nos[mask]

    out_chans=[]
    
    for chans in bad_chans:
        out_chans.append('-zap_chans')
        out_chans.append(chans)
        out_chans.append(chans+1)

    
    filterbank_name=filterbank.split('/')[-1].split('.')[0]
    out_dir='/ldata/trunk/{}/'.format(filterbank_name)
    _cmdline('mkdir -p {}'.format(out_dir))
    _cmdline(f'mv {filterbank} {out_dir}/')
    new_fil_path=f'{out_dir}{filterbank_name}.fil'

    heimdall_command='heimdall -nsamps_gulp 524288 -dm 10 10000 -boxcar_max 128 -cand_sep_filter 256 -cand_sep_dm_trial 20 -cand_sep_time 64 ' \
            + ''.join(str(x)+' ' for x in out_chans) + ' -output_dir {}'.format(out_dir) + ' -f {}'.format(new_fil_path)
    logging.info(f'Running {heimdall_command}')
    p1 = Process(target = _cmdline,args=[heimdall_command])
    p1.start()
    p2 = Process(target = write_and_plot,args=[mask,chan_nos,freqs,bandpass,out_dir])
    p2.start()

    p1.join()
    p2.join()

if __name__ == '__main__':
    parser=ArgumentParser(description='Stage 1: Get RFI Flags, run heimdall', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.add_argument('-d', '--daemon', dest='daemon', action='store_true', help='Run with AMQP')
    parser.add_argument('-n', '--nchans', type=int, help='no. of chans to calc. median over', default=64)
    parser.add_argument('-s', '--sigma', type=int, help='sigma over which values are tagged as RFI', default=3)
    parser.add_argument('-f', '--file', type=str, help='Filterbank file')
    parser.set_defaults(verbose=False)
    parser.set_defaults(daemon=True)
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
