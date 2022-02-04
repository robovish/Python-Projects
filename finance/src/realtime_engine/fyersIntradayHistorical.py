import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from fyers_api import fyersModel
import time as ts
from datetime import date, timedelta, datetime, time
import pandas as pd
from sqlalchemy.types import DateTime
from utils import db_conn, api_conn
import toml

mssql_engine, cnxn, cursor = db_conn.db_conn()

config = toml.load(r'D:\Python-Projects\finance\config.toml')

client_id = config['BROKER_FYERS']['CLIENT_ID']
redirect_uri = config['BROKER_FYERS']['REDIRECT_URI']
secret_key = config['BROKER_FYERS']['SECRET_KEY']
grant_type = config['BROKER_FYERS']['GRANT_TYPE']
response_type = config['BROKER_FYERS']['RESPONSE_TYPE']
state = config['BROKER_FYERS']['STATE']
log_path = config['BROKER_FYERS']['LOG_PATH']

session, access_token = api_conn.fyers_api_session(client_id, redirect_uri, secret_key, grant_type, response_type, state)

with open("""D:\\Python-Projects\\finance\\data\\access_token_fyers.txt""", 'r') as fl:
        access_token = fl.read()
        print (access_token)


fyers = fyersModel.FyersModel(client_id=client_id, token=access_token,log_path= log_path)

def fyers_symbol_feed():
        
    feed_list = []
    
    resolution_map = {'1': '5d', '3': '5d', '5': '10d', '15': '15d', '60' : '99d' , '240': '99d'}

    feed = {"symbol":"NSE:SBIN-EQ","resolution":"2","date_format":"1","range_from":"2022-01-27","range_to":"2022-01-27",
            "cont_flag":"1"}

    symbol_list = ["NSE:SBIN-EQ", "NSE:ONGC-EQ"]
    duration = [ '1','3', '5','15','60','240']
    start_date = (date.today() - timedelta(days=0)).strftime("%Y-%m-%d")
    end_date = (date.today() - timedelta(days=0)).strftime("%Y-%m-%d")
    print (start_date, end_date)

    for symbol in symbol_list:
        for res in duration:
            feed['symbol'] = symbol
            feed['resolution'] = res
            feed['date_format'] = '1'
            feed['range_from'] = start_date
            feed['range_to'] = end_date
            feed1 = feed.copy()
            feed_list.append(feed1)
#             print (feed_list)

    return (feed_list)


def fyers_historical_intraday(feed_list):
    
    for feed in feed_list:
        # Need to add try for token checking exception
        data = fyers.history(feed)
        print (feed)
        df = pd.DataFrame(data['candles'], columns = ['DATE','OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE', 'VOLUME'])
        df.sort_values(['DATE'], ascending=True, ignore_index=True).reset_index()
        df['DATE'] = pd.to_datetime(pd.to_datetime(df['DATE'], unit='s') + pd.Timedelta("05:30:00"))
        df['SYMBOL'] = feed['symbol']
        df['DURATION'] = feed['resolution']
        df['CREATED_DT'] = date.today()
        df = df[['SYMBOL','DATE','OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE', 'VOLUME', 'DURATION', 'CREATED_DT']]

        df.to_sql(name= 'intraday_history', con= mssql_engine, if_exists='append', index=False, dtype = {'DATE': DateTime})



fyers_historical_intraday(fyers_symbol_feed())