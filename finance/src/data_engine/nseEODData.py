import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import investpy as ip
import pandas as pd
import numpy as np
from datetime import date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.types import Date
import datetime as dt
import time
from nsepy import get_history
from utils import db_conn
import toml

config = toml.load(r'D:\Python-Projects\finance\config.toml')

nse_holidays = config['NSE']['NSE_HOLIDAYS']

# nse_holidays = ['2022-01-26', '2022-03-01', '2022-03-18', '2022-04-01', '2022-04-14', '2022-04-15', '2022-05-03', '2022-05-16', 
# '2022-08-09', '2022-08-15', '2022-08-16', '2022-08-31', '2022-10-05', '2022-10-24', '2022-10-26', '2022-11-08']

mssql_engine, cnxn, cursor = db_conn.db_conn()


def stock_daily_download():
    '''
    Function: Code to be run daily to insert rows for Stocks in Main DB using NSEPy
    '''

    qry = """SELECT DISTINCT SYMBOL, ISIN FROM master_india_stocks_list"""

    stock_list = pd.read_sql(qry, con=cnxn)

    dt = date.today()

    for i, val in stock_list.iterrows():

        search_result = get_history(symbol= val[0], start=date(dt.year,dt.month,dt.day), end=date(dt.year,dt.month,dt.day))

    #----------Queries NSEpy only when search result is returned---------------------
        if not(search_result.empty):
            print ("NSEPy: ", val[0])
            
            search_result.reset_index(inplace=True)
            search_result['Symbol'] = val[0]
            search_result['CREATED_DT'] = date.today()
            search_result['CHANGE_PCT'] = (((search_result['Close'] - search_result['Prev Close'])/search_result['Prev Close'])*100).round(2)
            search_result['%Deliverble'] = search_result['%Deliverble'].round(2)


            search_result = search_result[['Symbol','Date', 'Open', 'High', 'Low', 'Close', 'Prev Close', 'CHANGE_PCT',
                                        'Volume', 'Deliverable Volume', 'Turnover', 'Trades', '%Deliverble', 'VWAP' ,'CREATED_DT']]


            search_result.columns = ['SYMBOL', 'DATE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'PREV_CLOSE','CHANGE_PCT',
                                    'VOLUME', 'DELIVERABLE_VOLUME', 'TURNOVER', 'TRADES','DELIVERY_PCT', 'VWAP', 'CREATED_DT']

            search_result.to_sql(name= 'india_stocks_daily', con= mssql_engine, if_exists='append', index=False, dtype = {'CREATED_DT' : Date, 'DATE': Date})

            time.sleep(0.1)

    #------------Below code queries Investing.com Investpy package only for stocks for which data is not in NSEPy-----
        else:
            print ("Investpy: ", val[0])
            start_date = (date.today() - timedelta(days=3)).strftime("%d/%m/%Y")
            end_date = (date.today() + timedelta(days=0)).strftime("%d/%m/%Y")
            # print (start_date, end_date)
            
            try:
                search_result_ip = ip.search_quotes(text= val[1], products=['stocks'], countries=['india'], n_results=1).retrieve_historical_data(from_date= start_date, to_date = end_date)
                search_result_ip.reset_index(inplace=True)

                search_result_ip['Symbol'] = val[0]
                search_result_ip['CREATED_DT'] = date.today()
                search_result_ip['PREV_CLOSE'] = search_result_ip['Close'].shift(1)
                search_result_ip['DELIVERABLE_VOLUME'] = 0
                search_result_ip['TURNOVER'] = 0
                search_result_ip['TRADES'] = 0
                search_result_ip['DELIVERY_PCT'] = 0
                search_result_ip['VWAP'] = 0

                search_result_ip = search_result_ip[['Symbol','Date', 'Open', 'High', 'Low', 'Close', 'PREV_CLOSE', 'Change Pct', 
                                                    'Volume', 'DELIVERABLE_VOLUME', 'TURNOVER', 'TRADES','DELIVERY_PCT', 'VWAP', 'CREATED_DT']]


                search_result_ip.columns = ['SYMBOL', 'DATE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'PREV_CLOSE','CHANGE_PCT',
                                        'VOLUME','DELIVERABLE_VOLUME', 'TURNOVER', 'TRADES','DELIVERY_PCT', 'VWAP', 'CREATED_DT']

                search_result_ip = search_result_ip.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).tail(1)

                search_result_ip.to_sql(name= 'india_stocks_daily', con= mssql_engine, if_exists='append', index=False, dtype = {'CREATED_DT' : Date, 'DATE': Date})

                time.sleep(1)
            except:
                print("Not found: ", val[0])
                continue
                


def index_daily_download():

    '''
    Function: Code to Extract Historical Index data using Investpy
    '''
    index_list = pd.read_sql("""SELECT * FROM master_index_list where IS_AVAILABLE_INVESTING = 'Y'""", con=cnxn)

    start_date = date.today().strftime("%d/%m/%Y")
    end_date = (date.today() + timedelta(days=1)).strftime("%d/%m/%Y")

    dt = date.today() 
    print (dt) 

    for i, val in index_list.iterrows():
        print (val[0])
        try:
            search_result = ip.search_quotes(text=val[0], products=['indices'], countries=['india'], n_results=1).retrieve_historical_data(from_date = start_date, to_date = end_date)
            search_result.reset_index(inplace=True)
            search_result['SYMBOL'] = val[0]
            search_result['INDEX_TYPE'] = val[2]
            search_result['INDEX_SUBTYPE'] = val[3]
            search_result['COUNTRY'] = val[5]
            search_result['CREATED_DT'] = date.today()
            
            search_result = search_result[['SYMBOL', 'INDEX_TYPE', 'INDEX_SUBTYPE', 'COUNTRY', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change Pct', 'CREATED_DT']]
            
            search_result.columns = ['SYMBOL', 'INDEX_TYPE', 'INDEX_SUBTYPE', 'COUNTRY', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'CHANGE_PCT', 'CREATED_DT']
            
            search_result.to_sql(name= 'index_daily', con= mssql_engine, if_exists='append', index=False, dtype = {'CREATED_DT' : Date, 'DATE': Date})
            time.sleep(2)
        except:
            # log error msg
            print("Not found: ", val[0])
            continue


def stock_weekly_download():

    '''
    Function: Used to download Stock Data weekly once on Monday of every week from Investing.com for Previous week
    '''

    qry= """SELECT DISTINCT SYMBOL, ISIN FROM master_india_stocks_list"""
    
    stock_list = pd.read_sql(qry, con=cnxn)

    # This has to be second last week Sunday from current Monday - Reduce 15 days from now date when runs on Monday
    start_date = (date.today() - timedelta(days=17)).strftime("%d/%m/%Y")

    # This has to be last week Sunday from current Monday - Reduce 5 days from now date when runs on Monday
    end_date = (date.today() - timedelta(days=5)).strftime("%d/%m/%Y")

    print (start_date, end_date)

    stock_not_found = []

    for i, val in stock_list.iterrows():
        try:
            search_result = ip.search_quotes(text=val[1], products=['stocks'], countries=['india'], n_results=1)
            symbol = getattr(search_result,'symbol')
            print (val[0], '|' , symbol)
            search_result =ip.get_stock_historical_data(stock= symbol, country='India', interval = 'weekly', 
                                                    from_date=start_date, to_date=end_date)
            search_result.reset_index(inplace=True)
            search_result['Symbol'] = val[0]
            search_result['CREATED_DT'] = date.today()
            search_result['PREV_CLOSE'] = search_result['Close'].shift(1)
            search_result['CHANGE_PCT'] = (((search_result['Close'] - search_result['PREV_CLOSE'])/search_result['PREV_CLOSE'])*100).round(2)


            search_result = search_result[['Symbol','Date', 'Open', 'High', 'Low', 'Close', 'PREV_CLOSE', 'CHANGE_PCT',
                                        'Volume', 'CREATED_DT']]


            search_result.columns = ['SYMBOL', 'DATE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'PREV_CLOSE','CHANGE_PCT',
                                    'VOLUME', 'CREATED_DT']
            
            search_result = search_result.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).tail(1)
            
            # print (search_result)
            
            search_result.to_sql(name= 'india_stocks_weekly', con= mssql_engine, if_exists='append', index=False, dtype = {'CREATED_DT' : Date, 'DATE': Date})

            time.sleep(3)
        except:
            stock_not_found.append(val[0])
            print ("Weekly stock not found", val[0])
            continue

def stock_monthly_download():

    '''
    Function: Used to download Stock Data Monthly once on First of every Month from Investing.com for Previous month 
    '''

    qry= """SELECT DISTINCT SYMBOL, ISIN FROM master_india_stocks_list"""
    
    stock_list = pd.read_sql(qry, con=cnxn)

# Date logic to get the last months data so that prev month values can be loaded in DB after completion of Month.

    # dt = date.today() + timedelta(days=11)
    last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
    start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
    last_day_of_prev2_month = start_day_of_prev_month.replace(day=1) - timedelta(days=1)
    start_day_of_prev2_month = start_day_of_prev_month.replace(day=1) - timedelta(days=last_day_of_prev2_month.day)

    start_date = start_day_of_prev2_month.strftime("%d/%m/%Y")
    end_date = start_day_of_prev_month.strftime("%d/%m/%Y")

    # print (last_day_of_prev_month , start_day_of_prev_month)
    # print (last_day_of_prev2_month , start_day_of_prev2_month)
    print (start_date, end_date)

    stock_not_found = []

    for i,val in stock_list.iterrows():
        try:
            search_result = ip.search_quotes(text=val[1], products=['stocks'], countries=['india'], n_results=1)
            symbol = getattr(search_result,'symbol')
            print (val[0], symbol)
            search_result =ip.get_stock_historical_data(stock= symbol, country='India', interval = 'monthly', 
                                                    from_date=start_date, to_date=end_date)
            search_result.reset_index(inplace=True)
            search_result['Symbol'] = val[0]
            search_result['CREATED_DT'] = date.today()
            search_result['Prev Close'] = search_result['Close'].shift(1)
            search_result['CHANGE_PCT'] = (((search_result['Close'] - search_result['Prev Close'])/search_result['Prev Close'])*100).round(2)


            search_result = search_result[['Symbol','Date', 'Open', 'High', 'Low', 'Close', 'Prev Close', 'CHANGE_PCT',
                                        'Volume', 'CREATED_DT']]


            search_result.columns = ['SYMBOL', 'DATE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'PREV_CLOSE','CHANGE_PCT',
                                    'VOLUME', 'CREATED_DT']
            
            search_result = search_result.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).tail(1)
            
            # print (search_result)
            
            search_result.to_sql(name= 'india_stocks_monthly', con= mssql_engine, if_exists='append', index=False, dtype = {'CREATED_DT' : Date, 'DATE': Date})

            time.sleep(2)

        except:
            stock_not_found.append(val[0])
            print ("stock not found", val[0])
            continue





# stock_symbol_update_investpy()

# stock_daily_download()

# index_daily_download()

# stock_weekly_download()

# stock_monthly_download()


# Master logic to execute different functions as per various days conditions for download

if (str(date.today() + timedelta(0)) not in nse_holidays):
    if ((date.today() - timedelta(0)).day == 1) and (date.today() - timedelta(0)).strftime("%a") == 'Mon':
        print ("Execute Weekly, Monthly, Daily, Index, Stock Symbol function")
        stock_daily_download()
        index_daily_download()
        stock_weekly_download()
        stock_monthly_download()
    elif ((date.today() - timedelta(0)).day == 1 and (date.today() - timedelta(0)).strftime("%a") in ['Sun', 'Sat']):
        print ("Execute Monthly, Stock Symbol function")
        stock_monthly_download()
    elif ((date.today() - timedelta(0)).day == 1):
        print ("Execute Monthly, Daily, Index and Stock Symbol function")
        stock_daily_download()
        index_daily_download()
        stock_monthly_download()
    elif (date.today() - timedelta(0)).strftime("%a") == 'Mon':
        print ("Execute Weekly, Daily and Index")
        stock_daily_download()
        index_daily_download()
        stock_weekly_download()
    elif ((date.today() - timedelta(0)).strftime("%a") in  ['Tue', 'Wed', 'Thu', 'Fri']):
        print ("Execute Daily and Index function")
        stock_daily_download()
        index_daily_download()
    else: 
        print ('NO Execution as Day is:', (date.today() - timedelta(0)).strftime("%A"))
else:
    print ('Today is NSE Holiday')

    
cnxn.close()
