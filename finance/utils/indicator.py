import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import pandas as pd 
import pandas_ta as pta
from utils import db_conn

mssql_engine, cnxn, cursor = db_conn.db_conn()

def data_load(con):
    data = pd.read_sql("""SELECT * FROM india_stocks_daily isd WHERE DATE >= '2021-01-01' ORDER BY SYMBOL, DATE ASC""", con =cnxn)
    return data
# AND SYMBOL IN ('TCS', 'ADANIPORTS', 'GRANULES')

def indicators(data):
    """
    Function: 
    To apply various indicators to the data used for building multiple Screening criteria    
    """

    ttm_squeeze = pta.squeeze(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], bb_length= 21, bb_std=2, 
                              kc_length=21, kc_scalar=1.5,detailed=False, mamode= 'ema' )
    
    bbands = pta.bbands(data['CLOSE_PRICE'], length=21, std=2, talib=True, mamode='ema')
    
    keltner_channel = pta.kc(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], length=21, 
                             scalar=1.5, mamode='ema', offset=None)
    
    adx = pta.adx(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], length=21)    
    
    atr = pta.atr(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], length=21, mamode= 'ema')
    
    df = pd.concat([data, ttm_squeeze, bbands, keltner_channel,adx], axis= 1)
    
    df.set_index(pd.DatetimeIndex(df['DATE']), inplace=True)

    df.rename(columns={"OPEN_PRICE": "Open", "HIGH_PRICE": "High", "LOW_PRICE": "Low", "CLOSE_PRICE": "Close", "VOLUME": "Volume" },
         inplace=True)
    
    return df

def data_process(data):
    df =  data.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).apply(indicators).reset_index(drop=True)
    return df

# print (data_process(data_load(cnxn)))