#!/usr/bin/env python3


from subprocess import PIPE, Popen
import filutils
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging
from multiprocessing import Process
import glob
import numpy as np

__author__='Devansh Agarwal'
__email__ = 'da0017@mix.wvu.edu'

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
    return output

def write_and_plot(mask,chan_nos,freqs,bandpass,outdir):
    "Writes and plots the bandpass"
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



if __name__=='__main__':

    parser=ArgumentParser(description='Stage 1: Get RFI Flags, run heimdall', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.add_argument('-n', '--nchans', type=int, help='no. of chans to calc. median over', default=64)
    parser.add_argument('-s', '--sigma', type=int, help='sigma over which values are tagged as RFI', default=3)
    parser.add_argument(dest='file')
    parser.set_defaults(verbose=False)
    values = parser.parse_args()

    if values.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

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

    heimdall_command='tsp heimdall -dm 2 10000 -max_giant_rate 100 -boxcar_max 8 -cand_sep_filter 8 -cand_sep_dm_trial 20 -cand_sep_time 64 ' \
            + ''.join(str(x)+' ' for x in out_chans) + ' -output_dir {}'.format(out_dir) + ' -f {}'.format(filterbank)

    p1 = Process(target = _cmdline,args=[heimdall_command])
    p1.start()
    p2 = Process(target = write_and_plot,args=[mask,chan_nos,freqs,bandpass,out_dir])
    p2.start()
