import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import pandas as pd 
import pandas_ta as pta
from utils import db_conn

mssql_engine, cnxn, cursor = db_conn.db_conn()

def data_load(con, interval = 'daily'):
    if interval == 'daily':
            print ("Daily data")
            data = pd.read_sql("""SELECT * FROM india_stocks_daily WHERE DATE >= '2021-01-01' ORDER BY SYMBOL, DATE ASC""", con =cnxn)
            return data
    elif interval == 'weekly':
            print ("Weekly data")
            data = pd.read_sql("""SELECT * FROM india_stocks_weekly WHERE DATE >= '2015-01-01' ORDER BY SYMBOL, DATE ASC""", con =cnxn)
            return data
    elif interval == 'monthly':
            print ("Monthly data")
            data = pd.read_sql_query("""SELECT * FROM india_stocks_monthly WHERE DATE >= '2010-01-01' ORDER BY SYMBOL, DATE ASC""", con =cnxn)
            return data
    else:
            print ('Incorrect Parameters given')



def indicators(data):
    """
    Function: 
    To apply various indicators to the data used for building multiple Screening criteria    
    """
#     print (data.shape)
#     print (data['SYMBOL'])
#     data.set_index(pd.DatetimeIndex(data['DATE']), inplace=True)
    
    ttm_squeeze = pta.squeeze(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], bb_length= 21, bb_std=2, 
                              kc_length=21, kc_scalar=1.5, detailed=False, mamode= 'ema' )
    
    bbands = pta.bbands(data['CLOSE_PRICE'], length=21, std=2, talib=True, mamode='ema')
    
    
    keltner_channel = pta.kc(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], length=21, 
                             scalar=1.5, mamode='ema', offset=None)
    
    adx = pta.adx(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], length=21)    
    
    atr = pta.atr(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], length=21, mamode= 'ema')
    
    sma_20 = pta.ema(data['CLOSE_PRICE'], length=20)
    
    sma_34 = pta.ema(data['CLOSE_PRICE'], length=34)
    
    sma_50 = pta.ema(data['CLOSE_PRICE'], length=50)
    
    sma_100 = pta.ema(data['CLOSE_PRICE'], length=100)
    
    rsi = pta.rsi(data['CLOSE_PRICE'], length=14)
    
    AVG_VOL21 = pta.sma(data['VOLUME'], length=21)
    
#     increasing = pta.increasing(bbands['BBM_21_2.0'], strict = False, length=21, asint=False, offset=0)
    
#     longrun = pta.long_run(data['CLOSE_PRICE'], strict = False, length=5, asint=True, offset=0)
    
    slope = pta.slope(pta.ema(data['CLOSE_PRICE'], length=21), length=10, offset = -2)

#     vwap = pta.vwap(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], data['VOLUME'], fillna= None)
    
    df = pd.concat([data, ttm_squeeze, bbands, keltner_channel,adx, sma_20, sma_34, sma_50, sma_100, rsi, slope, AVG_VOL21], axis= 1)
    
    df.set_index(pd.DatetimeIndex(df['DATE']), inplace=True)
    
    df.rename(columns={"OPEN_PRICE": "Open", "HIGH_PRICE": "High", "LOW_PRICE": "Low", "CLOSE_PRICE": "Close", 
                       "VOLUME": "Volume", 'SMA_21': "VOL_AVG_21", 'SLOPE_10': "CLOSE_SLOPE_10" },inplace=True)
    
    return df


def data_process(data, interval='daily'):
    df =  data.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).apply(indicators).reset_index(drop=True)
    # df['VOL_AVG_21'] = df['VOL_AVG_21'].astype('int64', errors='ignore')
    if interval == 'daily':
        df.to_sql('daily_stock_indicator_tmp', con=mssql_engine, index= False, if_exists='replace')
    elif interval == 'weekly':
        df.to_sql('weekly_stock_indicator_tmp', con=mssql_engine, index= False, if_exists='replace')
    elif interval == 'monthly':
        df.to_sql('monthly_stock_indicator_tmp', con=mssql_engine, index= False, if_exists='replace')
    else: 
        print ('Incorrect input!!')
    return (df)


# print (data_process(data_load(cnxn, interval = 'monthly'), interval='monthly'))