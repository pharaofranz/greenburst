#!/usr/bin/env python3

import numpy as np
import sys
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging

__author__='Devansh Agarwal'
__email__ = 'da0017@mix.wvu.edu'

parser=ArgumentParser(description='Find RFI in sigproc bandpass file using a running median', formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
parser.add_argument('-p', '--plot', action='store_true', help='Show plots', default=True)
parser.add_argument('-n', '--nchans', type=int, help='no. of chans to calc. median over', default=64)
parser.add_argument('-s', '--sigma', type=int, help='sigma over which values are tagged as RFI', default=3)
parser.add_argument(dest='file')
parser.set_defaults(verbose=False)
values = parser.parse_args()

if values.verbose:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)


def mask_finder(data,chan_nos,nchan_avg,sigma):
    total_mask=np.empty(data.shape[0],dtype=bool)*False
    
    for ii in range(int(data.shape[0]/nchan_avg)):
        median=np.median(data[ii*nchan_avg:(ii+1)*nchan_avg-1])
        total_mask[ii*nchan_avg:(ii+1)*nchan_avg-1]=data[ii*nchan_avg:(ii+1)*nchan_avg-1]>=(sigma*median)
    return total_mask


data=np.loadtxt(values.file,usecols=[1])
chan_nos=np.arange(1,data.shape[0]+1)

total_mask=mask_finder(data,chan_nos,values.nchans,values.sigma)

logging.info('Flagged %d channels',total_mask.sum())

if values.plot:
    import pylab as plt 
    plt.plot(chan_nos,data,'k-',label="Bandpass")
    plt.plot(chan_nos[total_mask],data[total_mask],'ro',label="Flaged Channels")
    plt.xlabel("Chan. no.")
    plt.ylabel("Arb. Units")
    plt.yscale("log")
    plt.title(sys.argv[1])
    plt.legend()
    plt.savefig(sys.argv[1]+'.pdf',bbox_inches='tight')
    plt.show()

with open("bad_chans.aux",'w') as f:
    np.savetxt(f,chan_nos[total_mask],fmt='%d')
