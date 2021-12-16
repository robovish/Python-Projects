def data_series_scale(data, val=[]):
    """Function: Used for scaling the data between 0 and 1. Used for Scaling various Indexes to same scale"""
    val = (data['CLOSE']-data['CLOSE'].min())/(data['CLOSE'].max()-data['CLOSE'].min())
    val.append(val)
    return (val)