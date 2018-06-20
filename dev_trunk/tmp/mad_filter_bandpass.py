#!/usr/bin/env python3

import numpy as np
import sys
import pylab as plt

freq,val=np.loadtxt(sys.argv[1],unpack=True)
#val=10*np.log10(val)

def get_median_filtered(signal, threshold=3):
    signal = signal.copy()
    difference = np.abs(signal - np.median(signal))
    median_difference = np.median(difference)
    if median_difference == 0:
        s = 0
    else:
        s = difference / float(median_difference)
    mask = s > threshold
    signal[mask] = np.median(signal)
    return signal,mask

filter_data,mask=get_median_filtered(val)

plt.plot(val,'k.')
plt.plot(val[~mask],'r.')
plt.show()
