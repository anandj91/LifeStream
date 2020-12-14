import os
import csv
import pandas as pd
from biosppy.signals.tools import filter_signal
from biosppy.signals.tools import get_filter
import numpy as np
import sys
from scipy import stats
from scipy.interpolate import interp1d
import scipy.signal as ss
import time
from sklearn.preprocessing import StandardScaler

DURATION = int(sys.argv[1])

class Signal:
    def __init__(self, ts, val):
        self.ts = ts
        self.val = val

class CSVSignal:
    def __init__(self, filenames, window, period):
        self.filenames = filenames
        self.window = window
        self.period = period
        self.count = -1
        self.idx = 0
        length = window//period
        self.size = ((1000//period)*DURATION)//length
        self.data = []
        for i in range(0, self.size):
            self.data.append(Signal(i*window, [float(i*window+l) for l in range(length)]))

    def _get_data(self, filename, window, period, data):
        length = window//period
        sig = data[-1]
        for i in range(0, self.batch_size * window, period):
            ts = i
            val = float(i)
            if ts//window > sig.ts//window:
                data.append(Signal((ts//window)*window, [None for _ in range(length)]))
                sig = data[-1]
            idx = (ts - sig.ts)//period
            sig.val[idx] = val
    
        return data

    def _next_file(self):
        self.count += 1
        if self.count < len(self.filenames):
            return self.filenames[self.count]
        else:
            return ""

    def next(self):
        d = None
        if self.idx < len(self.data):
            d = self.data[self.idx]
            self.idx += 1
        return d

def normalize(waveform):
    waveform = np.array(waveform, dtype=np.float)
    scaler = StandardScaler()
    # Fit scaler with finite data
    scaler = scaler.fit(waveform.reshape(-1, 1))
    # Scale signal
    return scaler.transform(waveform.reshape(-1, 1)).reshape(-1,)

def filter(signal):
    signal = np.array(signal, dtype=np.float)
    filtered, _, _ = filter_signal(signal=signal, ftype='FIR', band='bandpass', order=150,
	    frequency=(2, 200), sampling_rate=500)
    return filtered

def imputeconst(signal, val=0):
    signal = np.array(signal, dtype=np.float)
    signal[np.where(np.isnan(signal))[0]] = val
    return signal

def imputemean(signal):
    signal = np.array(signal, dtype=np.float)
    signal[np.where(np.isnan(signal))[0]] = np.nanmean(signal)
    return signal

def upsample(signal, ip, op):
    assert ip//op > 0
    signal = np.array(signal, dtype=np.float)
    return ss.resample(signal, len(signal) * (ip//op))

def upsample_lininterp(signal, ip=2, op=1):
    scale = ip//op;
    signal = np.array(signal, dtype=np.float)
    x = np.arange(len(signal)) * scale
    y = signal
    f = interp1d(x, y)
    x_new = np.arange((len(signal)-1)*scale)
    return f(x_new)

def endtoend(window, ecg, abp):
    def ecg_transform(sig):
        if sig is None:
            return None
        sig.val = imputemean(sig.val)
        sig.val = upsample_lininterp(sig.val, 8, 2)
        sig.val = normalize(sig.val)
    
    def abp_transform(sig):
        if sig is None:
            return None
        sig.val = imputemean(sig.val)
        sig.val = normalize(sig.val)
    
    ecg_sig = ecg.next()
    ecg_transform(ecg_sig)
    abp_sig = abp.next()
    abp_transform(abp_sig)
    
    while ecg_sig is not None and abp_sig is not None:
        if ecg_sig.ts == abp_sig.ts:
            abp_transform(abp_sig)
            ecg_transform(ecg_sig)
            joined = np.concatenate(([ecg_sig.val],[abp_sig.val]), axis=1)
            abp_sig = abp.next()
            ecg_sig = ecg.next()
        elif ecg_sig.ts > abp_sig.ts:
            abp_sig = abp.next()
        else:
            ecg_sig = ecg.next()

def opbench(window, ecg, bench_fn):
    ecg_sig = ecg.next()
    while ecg_sig is not None:
        ecg_sig.val = bench_fn(ecg_sig.val)
        ecg_sig = ecg.next()

window = 60000
bench = sys.argv[2]

opbenchs = {
    "normalize": normalize,
    "passfilter": filter,
    "fillconst": imputeconst,
    "fillmean": imputemean,
    "resample": upsample_lininterp,
}

ecg = CSVSignal([], window, 2)
count = 0
start = time.time()
if bench == "endtoend":
    ecg = CSVSignal([], window, 2)
    abp = CSVSignal([], window, 8)
    count = DURATION * (500 + 125)
    endtoend(window, ecg, abp)
elif bench in opbenchs:
    count = DURATION * 500
    opbench(window, ecg, opbenchs[bench])
else:
    print("Unknown benchmark combination %s on NumLib" % (bench))
    exit(1)
end = time.time()
print("Benchmark: %s, Engine: NumLib, Data: %s million events, Time: %.3f sec" % (bench, count/1000000, end-start))
