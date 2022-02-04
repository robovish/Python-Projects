import numpy as np

#  custom logic for applying markers for diff conditions

def marker_ttm_sqz(sqz_on, price):
    signal   = []

    for date,value in sqz_on.iteritems():
        if value == 1:
            signal.append(price[date]*0.99)
        else:
            signal.append(np.nan)
        previous = value
    return signal


def marker_bb_overupper_2sig(bbu, high):
    signal   = []

    for date,value in bbu.iteritems():
        if high[date] > value:
            signal.append(high[date]*0.99)
        else:
            signal.append(np.nan)
        previous = value
    return signal


def marker_bb_overlower_2sig(bbl, low):
    signal   = []

    for date,value in bbl.iteritems():
        if low[date] < value:
            signal.append(low[date]*0.99)
        else:
            signal.append(np.nan)
        previous = value
    return signal
    
