#!/usr/bin/env python3

import matplotlib as mtb
import pylab as plt
import numpy as np
import pysigproc, filutils
from scipy.signal import argrelextrema
import operator

__author__='Devansh Agarwal'
__email__ ='da0017@mix.wvu.edu'

def SNR(signal):
    max_index, max_value = max(enumerate(signal), key=operator.itemgetter(1))
    leftsignal = signal[0:max_index];
    rightsignal = signal[max_index:];

    leftMin = np.array(leftsignal);
    rightMin = np.array(rightsignal);

    findLMin = argrelextrema(leftMin, np.less)[0][-1];
    findRMin = argrelextrema(rightMin, np.less)[0][0]+len(leftsignal);

    Anoise = np.std(list(signal[0:findLMin])+list(signal[findRMin:])) 
    Asignal = np.max(signal[findLMin:findRMin]) - np.mean(list(signal[0:findLMin])+list(signal[findRMin:]))
    snr_value = (Asignal/Anoise)
    return snr_value

def gb_plotter(first_image,mask,freqs,ts,dm,name):
    gridsize = (4, 11)
    fig = plt.figure(figsize=(22, 8))
    freqs=freqs[::-1]
    extent=[ts[0],ts[-1],freqs[0],freqs[-1]]
    bandpass_1 = plt.subplot2grid(gridsize, (0, 0), colspan=1, rowspan=3)
    bandpass_1.plot(first_image.mean(1)[::-1],freqs)
    bandpass_1.set_ylabel("Frequency")
    bandpass_1.invert_xaxis()
    
    timeseries_1 = plt.subplot2grid(gridsize, (3, 1), colspan=3, rowspan=1)
    timeseries_1.plot(ts,first_image.mean(0))
    timeseries_1.set_xlabel("Time")

    image_1 = plt.subplot2grid(gridsize, (0, 1), colspan=3, rowspan=3,sharey=bandpass_1,sharex=timeseries_1)
    image_1.imshow(first_image,aspect='auto',cmap='gist_heat',interpolation='none', extent=extent)
    image_1.set_title("Candidate")
    
    ###########################################
    
    sec_image=first_image*1.0*mask
    image_2 = plt.subplot2grid(gridsize, (0, 4), colspan=3, rowspan=3, sharey=bandpass_1,sharex=timeseries_1)
    image_2.imshow(mask,aspect='auto',cmap='binary',interpolation='none',extent=extent)
    image_2.set_title("Mask")
    
    ###########################################

    bandpass_3 = plt.subplot2grid(gridsize, (0, 10), colspan=1, rowspan=3,sharey=bandpass_1)
    bandpass_3.plot(sec_image.mean(1)[::-1],freqs)
    bandpass_3.yaxis.tick_right()
    bandpass_3.yaxis.set_label_position("right")
    bandpass_3.set_ylabel("Frequency")

    image_3 = plt.subplot2grid(gridsize, (0, 7), colspan=3, rowspan=3, sharey=bandpass_1,sharex=timeseries_1)
    image_3.imshow(sec_image,aspect='auto',cmap='gist_heat',interpolation='none',extent=extent)
    image_3.set_title("Candidate")

    timeseries_3 = plt.subplot2grid(gridsize, (3, 7), colspan=3, rowspan=1, sharex=timeseries_1)
    timeseries_3.plot(ts,sec_image.mean(0))
    timeseries_3.set_xlabel("Time")

    ###########################################
   
    timeseries_2 = plt.subplot2grid(gridsize, (3, 4), colspan=3, rowspan=1)
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.35)
    snr_pre=10.0 #SNR(first_image.mean(0))
    snr_post=10.0 #SNR(sec_image.mean(0))
    text='DM : '+'{:6.2f}'.format(dm)+'\nS/N (pre flagging): '+'{:6.2f}'.format(snr_pre)+'\nS/N (post flagging): '\
    +'{:6.2f}'.format(snr_post)
    timeseries_2.text(0.1, 0.9, text, transform=timeseries_2.transAxes, fontsize=14,
        verticalalignment='top', bbox=props)
    timeseries_2.axis('off')

    ###########################################
    
    plt.tight_layout()
    fig.subplots_adjust(hspace=0)   
    fig.subplots_adjust(wspace=0)
    for ax in [image_1, image_2, image_3]:
        plt.setp(ax.get_xticklabels(), visible=False)
        plt.setp(ax.get_yticklabels(), visible=False)
        # The y-ticks will overlap with "hspace=0", so we'll hide the bottom tick
    #    ax.set_yticks(ax.get_yticks()[1:]) 
    plt.savefig(name,bbox_inches='tight')

#105.67	486375	124.512	6	1	2.2913	125	485677	487453

fil_file='/sdata/filterbank/frb_2018-06-20_13-29-56.fil'
img,freqs,ts,tsmap=filutils.fil_data(fil_file,124.512-0.5,1)
dm=2.2913
mask=np.ones(img.T.shape)*True
img=filutils.dedisp(img,dm,freqs,tsmap,len(freqs))
gb_plotter(img.T,mask,freqs,ts,dm,'some.png')
