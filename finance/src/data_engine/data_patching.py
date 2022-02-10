"""
This script is used to store functions for patching data in DB as and when needed for specific symbols and duration.
"""
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import pandas as pd
from datetime import date, timedelta
import time
import os
from utils import db_conn
import toml 
from sqlalchemy.types import Date
import investpy as ip
from nsepy import get_history

mssql_engine, cnxn, cursor = db_conn.db_conn()

def adhoc_data_patch_daily():

    """
    Function: 
    This function is same as normal daily download function. But to do adhoc data updates with specific dates and 
    time this will be used so as to avoid affecting main function. For this query needs to be updated with list of symbols
    to operate upon.
    
    Comment: Check settings before execution for tail, date and sql tbl.
    """
    
    qry = """SELECT DISTINCT SYMBOL, ISIN FROM master_india_stocks_list"""

    stock_list = pd.read_sql(qry, con=cnxn)

    dt = date.today()
    
    for i, val in stock_list.iterrows():
        
        start_date = date(2000,1,1)
        end_date = date(2022,1,25)
        
        search_result = get_history(symbol= val[0], start=start_date, end=end_date)

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
            
            print (search_result)
#             search_result.to_sql(name= 'india_stocks_daily1', con= mssql_engine, if_exists='append', index=False, dtype = {'CREATED_DT' : Date, 'DATE': Date})

    #------------Below code queries Investing.com Investpy package only for stocks for which data is not in NSEPy-----
        else:
            print ("Investpy: ", val[0], val[1])
#             start_date = (date.today() - timedelta(days=1)).strftime("%d/%m/%Y")
#             end_date = (date.today() + timedelta(days=1)).strftime("%d/%m/%Y")
            
            start_date = '1/1/2021'
            end_date = '25/1/2022'
            print (start_date, end_date)

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

#                 search_result_ip = search_result_ip.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).tail(1)
                print (search_result_ip)
    
#                 search_result_ip.to_sql(name= 'india_stocks_daily1', con= mssql_engine, if_exists='append', index=False, dtype = {'CREATED_DT' : Date, 'DATE': Date})
            except:
                print("Not found: ", val[0])
                continue
                time.sleep(5)


def adhoc_data_patch_weekly():

    """
    Function: 
    This function is same as normal weekly download function. But to do adhoc data updates with specific symbols, dates and 
    time this will be used so as to avoid affecting main function. For this query needs to be updated with list of symbols
    to operate upon.
    
    Comment: Check settings before execution for tail, date and sql tbl.
    """

    qry= """SELECT DISTINCT SYMBOL, ISIN FROM master_india_stocks_list WHERE SYMBOL IN 
    (
    
    )"""

    stock_list = pd.read_sql(qry, con=cnxn)

    # This has to be second last week Sunday from current Monday - Reduce 15 days from now date when runs on Monday
    # start_date = (date.today() - timedelta(days=16)).strftime("%d/%m/%Y")

    # This has to be last week Sunday from current Monday - Reduce 8 days from now date when runs on Monday
    # end_date = (date.today() - timedelta(days=7)).strftime("%d/%m/%Y")

    start_date = '1/1/2000'
    end_date = '25/1/2022'
    print (start_date, end_date)

    stock_not_found = []

    for i,val in stock_list.iterrows():
    #     print (val[0])
        try:
            search_result = ip.search_quotes(text=val[1], products=['stocks'], countries=['india'], n_results=1)
            symbol = getattr(search_result,'symbol')
            print (val[0], '|' , symbol, '|', val[1])
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

    #         search_result = search_result.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).tail(1)

    #         print (search_result)
    #         search_result.to_sql(name= 'india_stocks_weekly1', con= mssql_engine, if_exists='append', index=False, dtype = {'CREATED_DT' : Date, 'DATE': Date})

            time.sleep(3)
        except:
            stock_not_found.append(val[0])
            print ("Weekly stock not found", val[0])
            continue

    print (stock_not_found)

    

def adhoc_data_patch_monthly():

    """
    Function: 
    This function is same as normal monthly download function. But to do adhoc data updates with specific dates and 
    time this will be used so as to avoid affecting main function. For this query needs to be updated with list of symbols
    to operate upon.
    
    Comment: Check settings before execution for tail, date and sql tbl.
    """

    qry = """SELECT SYMBOL, ISIN FROM master_india_stocks_list WHERE SYMBOL IN 
    (

    )"""

    stock_list = pd.read_sql(qry, con=cnxn)

    # Date logic to get the last months data so that prev month values can be loaded in DB after completion of Month.

#     last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
#     start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
#     last_day_of_prev2_month = start_day_of_prev_month.replace(day=1) - timedelta(days=1)
#     start_day_of_prev2_month = start_day_of_prev_month.replace(day=1) - timedelta(days=last_day_of_prev2_month.day)

#     start_date = start_day_of_prev2_month.strftime("%d/%m/%Y")
#     end_date = start_day_of_prev_month.strftime("%d/%m/%Y")

#     print (last_day_of_prev_month , start_day_of_prev_month)
#     print (last_day_of_prev2_month , start_day_of_prev2_month)

    start_date = '1/1/2000'
    end_date = '31/12/2021'
    
    print (start_date, end_date)

    stock_not_found = []

    for i,val in stock_list.iterrows():
    #     print (val[0])
        try:
            search_result = ip.search_quotes(text=val[1], products=['stocks'], countries=['india'], n_results=1)
            symbol = getattr(search_result,'symbol')
            print (val[0], '|', symbol, '|', val[1])
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

#             search_result = search_result.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).tail(1)
            
#             print (search_result)
        
#             search_result.to_sql(name= 'india_stocks_monthly', con= mssql_engine, if_exists='append', index=False, dtype = {'CREATED_DT' : Date, 'DATE': Date})

            time.sleep(5)
        except:
            stock_not_found.append(val[0])
            print ("stock not found", val[0])
            continue



# adhoc_data_patch_daily()

# adhoc_data_patch_weekly()

# adhoc_data_patch_monthly()