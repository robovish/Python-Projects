import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from datetime import date, timedelta
import pandas as pd
from utils import alerts, db_conn
import indicator
from data_engine import query
import toml 


config = toml.load(r'D:\Python-Projects\finance\config.toml')

nse_holidays = config['NSE']['NSE_HOLIDAYS']

# nse_holidays = ['2022-01-26', '2022-03-01', '2022-03-18', '2022-04-01', '2022-04-14', '2022-04-15', '2022-05-03', '2022-05-16', 
# '2022-08-09', '2022-08-15', '2022-08-16', '2022-08-31', '2022-10-05', '2022-10-24', '2022-10-26', '2022-11-08']

mssql_engine, cnxn, cursor = db_conn.db_conn()


# data_daily = indicator.data_process(indicator.data_load(cnxn, interval = 'daily'), interval='daily')
# data_weekly = indicator.data_process(indicator.data_load(cnxn, interval = 'weekly'), interval='weekly')
# data_monthly = indicator.data_process(indicator.data_load(cnxn, interval = 'monthly'), interval='monthly')

##---------------Master logic to execute different functions as per various days conditions for Screening

if (str(date.today() + timedelta(0)) not in nse_holidays):
    if ((date.today() - timedelta(0)).day == 1) and (date.today() - timedelta(0)).strftime("%a") == 'Mon':
        print ("Execute Weekly, Monthly, Daily")
        data_daily = indicator.data_process(indicator.data_load(cnxn, interval = 'daily'), interval='daily')
        data_weekly = indicator.data_process(indicator.data_load(cnxn, interval = 'weekly'), interval='weekly')
        data_monthly = indicator.data_process(indicator.data_load(cnxn, interval = 'monthly'), interval='monthly')

    elif ((date.today() - timedelta(0)).day == 1 and (date.today() - timedelta(0)).strftime("%a") in ['Sun', 'Sat']):
        print ("Execute Monthly only")
        data_monthly = indicator.data_process(indicator.data_load(cnxn, interval = 'monthly'), interval='monthly')

    elif ((date.today() - timedelta(0)).day == 1):
        print ("Execute Monthly and Daily")
        data_daily = indicator.data_process(indicator.data_load(cnxn, interval = 'daily'), interval='daily')
        data_monthly = indicator.data_process(indicator.data_load(cnxn, interval = 'monthly'), interval='monthly')

    elif (date.today() - timedelta(0)).strftime("%a") == 'Mon':
        print ("Execute Weekly and Daily only")
        data_daily = indicator.data_process(indicator.data_load(cnxn, interval = 'daily'), interval='daily')
        data_weekly = indicator.data_process(indicator.data_load(cnxn, interval = 'weekly'), interval='weekly')

    elif ((date.today() - timedelta(0)).strftime("%a") in  ['Tue', 'Wed', 'Thu', 'Fri']):
        print ("Execute Daily only")
        data_daily = indicator.data_process(indicator.data_load(cnxn, interval = 'daily'), interval='daily')

    else: 
        print ('NO Execution as Day is:', (date.today() - timedelta(0)).strftime("%A"))
else:
    print ('Today is NSE Holiday')



def insert_screener_tg(symbol_list, lookback_period, interval, screener_name = None, screener_criteria=None):
    
    param_cnt = ','.join(['?']*len(symbol_list))

    if interval == 'daily':
        qry = """SELECT SYMBOL, DATE, CLOSE_PRICE, VOLUME, CHANGE_PCT FROM india_stocks_daily WHERE DATE = ? AND SYMBOL IN ({}) """.format(param_cnt)
        df = pd.read_sql_query(qry, params= ((date.today() - timedelta(days=lookback_period)), *list(symbol_list)) , con=cnxn)
    elif interval == 'weekly':
        qry = """SELECT SYMBOL, DATE, CLOSE_PRICE, VOLUME, CHANGE_PCT FROM india_stocks_weekly WHERE DATE >= ? AND SYMBOL IN ({}) """.format(param_cnt)
        df = pd.read_sql_query(qry, params= ((date.today() - timedelta(days=lookback_period)), *list(symbol_list)) , con=cnxn)
    elif interval == 'monthly':
        qry = """SELECT SYMBOL, DATE, CLOSE_PRICE, VOLUME, CHANGE_PCT FROM india_stocks_monthly WHERE DATE >= ? AND SYMBOL IN ({}) """.format(param_cnt)
        df = pd.read_sql_query(qry, params= ((date.today() - timedelta(days=lookback_period)), *list(symbol_list)) , con=cnxn)
    else:
        print("Incorrect input")
        
    df['SCREENER'] = screener_name
    df['SCREENER_CRITERIA'] = screener_criteria
    df['CREATED_DT'] = date.today()

    df.to_sql("master_india_screener", con = mssql_engine, if_exists='append', index=False )
    print (df)

    msg = alerts.tg_msg_foramtter(screener_name + " | " + screener_criteria, df[['SYMBOL','CLOSE_PRICE','CHANGE_PCT']])
    alerts.tg_send_msg(msg)



def pct4_chg_daily(qry, change_pct = 4, price_range=50, vol=100000):

    params = (change_pct, vol, price_range)

    df = pd.read_sql(qry, params= params, con =cnxn)
    df['SCREENER'] = "PRICE_CHANGEPCT"
    df['SCREENER_CRITERIA'] = "Change Pct " + str(change_pct) + " Price Range: " + str(price_range) + " Volume: " + str(vol) 
    df['CREATED_DT'] = date.today()

    df.to_sql("master_india_screener", con = mssql_engine, if_exists='append', index=False )

    msg = alerts.tg_msg_foramtter("% Change >" + str(change_pct) + "| Vol >" + str(vol) + "| Close >" + str(price_range), df[['SYMBOL','CLOSE_PRICE','CHANGE_PCT']])
    alerts.tg_send_msg(msg)


def wk_ttm_sqz(data, lookback_period=6, price_range=50, vol=10000):
    
    '''
    Function: 
    Calculate TTM SQZ - WEEKLY level for Breakout and SQZ_ON till Wk-1. Inserts data into master_india_screener tbl and sends
    Telegram msg for each screener type
    
    Args:
    data: Data to be supplied for different intervals
    lookback_period: Period to be looked for SQZ_ON condition 
    price_range: Minimum price stocks to be filtered 
    vol: Minimum vol stocks to be filtered 
    
    Returns:
    None
    '''
    symbol_list_sqz = []
    symbol_list_breakout = []

    df2 = data.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).tail(lookback_period)

    df2.loc[:,'FILTER'] = (df2.SQZ_ON == 1) & (df2.Close > price_range) & (df2.VOL_AVG_21 > vol)

    df_grp = df2.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']) 

    for i, val in df_grp:
        if (len(val.iloc[:,0]) >= lookback_period):
            if (val.iloc[-1,-1] == True) & (val.iloc[-2,-1] == True) & (val.iloc[-3,-1] == True) & (val.iloc[-4,-1] == True) & (val.iloc[-5,-1] == True)  & (val.iloc[-6,-1] == True):
    #             print (i, 'In sqz')
                symbol_list_sqz.append(i)
            elif (val.iloc[-1,-1] == False) & (val.iloc[-2,-1] == True) & (val.iloc[-3,-1] == True) & (val.iloc[-4,-1] == True) & (val.iloc[-5,-1] == True)  & (val.iloc[-6,-1] == True):
    #             print (i, 'Breakout')
                symbol_list_breakout.append(i)
            else:
                pass
        else:
            pass 
            # print ("Stock doesn't have sufficient data: ", i )


    screener_criteria1 = "Lookback Days: " + str(lookback_period) + " | Price >= " + str(price_range) + " | Avg_Vol(21): " + str(vol) 
    print (screener_criteria1)
    insert_screener_tg(symbol_list_breakout, lookback_period = 12, interval='weekly', screener_name = "TTM_SQZ_WK_BREAKOUT", screener_criteria = screener_criteria1)

    screener_criteria2 = "Lookback Days: " + str(lookback_period) + " | Price >= " + str(price_range) + " | Avg_Vol(21): " + str(vol) 
    print (screener_criteria2)
    insert_screener_tg(symbol_list_sqz, lookback_period = 12, interval='weekly', screener_name = "TTM_SQZ_WK", screener_criteria = screener_criteria2)

    # return (df_ttm_breakout, df_ttm_sqz_on )
    return None



def month_ttm_sqz(data, lookback_period=4, price_range=50, vol=10000):
    
    '''
    Function: 
    Calculate TTM SQZ - Monthly level for Breakout and SQZ_ON till Month-1. Inserts data into master_india_screener tbl and sends
    Telegram msg for each screener type
    
    Args:
    data: Data to be supplied for different intervals
    lookback_period: Period to be looked for SQZ_ON condition 
    price_range: Minimum price stocks to be filtered 
    vol: Minimum vol stocks to be filtered 
    
    Returns:
    None
    '''
    symbol_list_sqz = []
    symbol_list_breakout = []

    df2 = data.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).tail(lookback_period)
    df2.loc[:,'FILTER'] = (df2.SQZ_ON == 1) & (df2.Close > price_range) & (df2.VOL_AVG_21 > vol)
    
    df_grp = df2.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']) 

    for i, val in df_grp:
        if (len(val.iloc[:,0]) >= lookback_period):
            if ((val.iloc[-1,-1] == True) & (val.iloc[-2,-1] == True) & (val.iloc[-3,-1] == True) & (val.iloc[-4,-1] == True)):
#                 & (val.iloc[-5,-1] == True) & (val.iloc[-6,-1] == True)):
#                 print (i, 'In sqz')
                symbol_list_sqz.append(i)
            elif ((val.iloc[-1,-1] == False) & (val.iloc[-2,-1] == True) & (val.iloc[-3,-1] == True) & (val.iloc[-4,-1] == True)):
#             & (val.iloc[-5,-1] == True) & (val.iloc[-6,-1] == True)):
#                 print (i, 'Breakout')
                symbol_list_breakout.append(i)
            else:
                pass
        else:
            pass
            # print ("Stock doesn't have sufficient data: ", i )


    screener_criteria1 = "Lookback Days: " + str(lookback_period) + " | Price >= " + str(price_range) + " | Avg_Vol(21): " + str(vol) 
    print (screener_criteria1)
    insert_screener_tg(symbol_list_breakout, lookback_period = 40, interval='monthly', screener_name = "TTM_SQZ_MONTH_BREAKOUT", screener_criteria = screener_criteria1)


    screener_criteria2 = "Lookback Days: " + str(lookback_period) + " | Price >= " + str(price_range) + " | Avg_Vol(21): " + str(vol) 
    print (screener_criteria2)
    insert_screener_tg(symbol_list_sqz, lookback_period = 40, interval='monthly', screener_name = "TTM_SQZ_MONTH", screener_criteria = screener_criteria2)

    # return (df_ttm_breakout, df_ttm_sqz_on )
    return None



def daily_ttm_sqz(data, lookback_period=4, price_range=50, vol=10000):
    
    '''
    Function: 
    Calculate TTM SQZ - DAILY level for Breakout and SQZ_ON. Inserts data into master_india_screener tbl and sends
    Telegram msg for each screener type
    
    Args:
    data: Data to be supplied for different intervals
    lookback_period: Period to be looked for SQZ_ON condition 
    price_range: Minimum price stocks to be filtered 
    vol: Minimum vol stocks to be filtered 
    
    Returns:
    None
    '''
    symbol_list_sqz = []
    symbol_list_breakout = []

    df2 = data.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).tail(lookback_period)

    df2.loc[:,'FILTER'] = (df2.SQZ_ON == 1) & (df2.Close > price_range) & (df2.VOL_AVG_21 > vol)

    df_grp = df2.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']) 
    
    #----Below filter logic needs to be updated whenever default lookback period is changed in function arg.----
    
    for i, val in df_grp:
        if (len(val.iloc[:,0]) >= lookback_period):
            if ((val.iloc[-1,-1] == True) & (val.iloc[-2,-1] == True) & (val.iloc[-3,-1] == True) & (val.iloc[-4,-1] == True)):
    #             & (val.iloc[-5,-1] == True)):
    #             print (i, 'In sqz')
                symbol_list_sqz.append(i)
            elif ((val.iloc[-1,-1] == False) & (val.iloc[-2,-1] == True) & (val.iloc[-3,-1] == True) & (val.iloc[-4,-1] == True)):
    #             & (val.iloc[-5,-1] == True)):
    #             print (i, 'Breakout')
                symbol_list_breakout.append(i)
            else:
                pass
        else:
            pass 
            # print ("Stock doesn't have sufficient data: ", i )
    
    screener_criteria1 = "Lookback Days: " + str(lookback_period) + " | Price >= " + str(price_range) + " | Avg_Vol(21): " + str(vol) 
    print (screener_criteria1)
    insert_screener_tg(symbol_list_breakout, lookback_period = 0, interval='daily', screener_name = "TTM_SQZ_DAILY_BREAKOUT", screener_criteria = screener_criteria1)

    screener_criteria2 = "Lookback Days: " + str(lookback_period) + " | Price >= " + str(price_range) + " | Avg_Vol(21): " + str(vol) 
    print (screener_criteria2)
    insert_screener_tg(symbol_list_sqz, lookback_period = 0, interval='daily', screener_name = "TTM_SQZ_DAILY", screener_criteria = screener_criteria2)
 
    # return (df_ttm_breakout, df_ttm_sqz_on )
    return None




def trend_adx_daily(data, adx_criteria = 23, price_range=50, vol=500000):

    df_trend_adx = data.sort_values(by=['SYMBOL', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL']).tail(1)
    df_trend_adx1 = df_trend_adx.loc[(df_trend_adx['ADX_21'] > adx_criteria) & (df_trend_adx['Close'] > price_range)  & (df_trend_adx['VOL_AVG_21'] > vol)]['SYMBOL'].unique()
    df_trend_adx1 = df_trend_adx1.tolist()

    screener_criteria = "ADX >=  " + str(adx_criteria) + " | Price Range: " + str(price_range) + " | Avg_Vol(21): " + str(vol)
    print (screener_criteria)

    insert_screener_tg(df_trend_adx1, lookback_period = 0, interval='daily', screener_name = "TREND_ADX_DAILY", screener_criteria = screener_criteria)
    


# print (pct4_chg_daily(query.pct4_chg))
# print (trend_adx_daily(data_daily))
# print (wk_ttm_sqz(data_weekly))
# print (month_ttm_sqz(data_monthly))
# print (daily_ttm_sqz(data_daily))


if (str(date.today() + timedelta(0)) not in nse_holidays):
    if ((date.today() - timedelta(0)).day == 1) and (date.today() - timedelta(0)).strftime("%a") == 'Mon':
        print ("Execute Weekly, Monthly, Daily")
        print (wk_ttm_sqz(data_weekly))
        print (month_ttm_sqz(data_monthly))
        print (daily_ttm_sqz(data_daily))
        print (pct4_chg_daily(query.pct4_chg))
        print (trend_adx_daily(data_daily))

    elif ((date.today() - timedelta(0)).day == 1 and (date.today() - timedelta(0)).strftime("%a") in ['Sun', 'Sat']):
        print ("Execute Monthly only")
        print (month_ttm_sqz(data_monthly))

    elif ((date.today() - timedelta(0)).day == 1):
        print ("Execute Monthly and Daily")
        print (month_ttm_sqz(data_monthly))
        print (daily_ttm_sqz(data_daily))
        print (pct4_chg_daily(query.pct4_chg))
        print (trend_adx_daily(data_daily))

    elif (date.today() - timedelta(0)).strftime("%a") == 'Mon':
        print ("Execute Weekly and Daily only")
        print (wk_ttm_sqz(data_weekly))
        print (daily_ttm_sqz(data_daily))
        print (pct4_chg_daily(query.pct4_chg))
        print (trend_adx_daily(data_daily))

    elif ((date.today() - timedelta(0)).strftime("%a") in  ['Tue', 'Wed', 'Thu', 'Fri']):
        print ("Execute Daily only")
        print (daily_ttm_sqz(data_daily))
        print (pct4_chg_daily(query.pct4_chg))
        print (trend_adx_daily(data_daily))

    else: 
        print ('NO Execution as Day is:', (date.today() - timedelta(0)).strftime("%A"))
else:
    print ('Today is NSE Holiday')

cnxn.close()