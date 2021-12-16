import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from datetime import date, timedelta
import pandas as pd
from utils import alerts, db_conn, indicator
from data import query


mssql_engine, cnxn, cursor = db_conn.db_conn()

#Import Data for Screening Stocks for Last 1 year
data = indicator.data_process(indicator.data_load(cnxn))
data.to_sql('daily_stock_indicator_tmp', con=mssql_engine, index= False, if_exists='replace')

def pct4_chg(qry, change_pct = 4, price_range=50, vol=2000000):
    params = (change_pct, vol, price_range)

    df = pd.read_sql(qry, params= params, con =cnxn)

    df['SCREENER'] = "PRICE_CHANGEPCT"
    df['SCREENER_CRITERIA'] = "Change Pct " + str(change_pct) + " Price Range: " + str(price_range) + " Volume: " + str(vol) 
    df['CREATED_DT'] = date.today()

    df.to_sql("daily_india_screener", con = mssql_engine, if_exists='append', index=False )

    msg = alerts.tg_msg_foramtter("% Change >" + str(change_pct) + "| Vol >" + str(vol) + "| Close >" + str(price_range), df[['SYMBOL','CLOSE_PRICE','CHANGE_PCT']])
    alerts.tg_send_msg(msg)


def ttm_sqz(data, lookback_period=8, price_range=50, vol=2000000):

    dt = date.today() - timedelta(days=8)
    data = data.loc[(data['DATE'] >= date(dt.year,dt.month,dt.day)) & (data['SQZ_ON']==1) & \
                  (data['Close'] > 50) & (data['Volume'] > 2000000)]['SYMBOL'].unique()

    param_cnt = ','.join(['?']*len(data))
    qry = """SELECT SYMBOL, CLOSE_PRICE, VOLUME, CHANGE_PCT FROM india_stocks_daily WHERE DATE = ? AND SYMBOL IN ({}) """.format(param_cnt)
    df = pd.read_sql_query(qry, params= ((date.today() - timedelta(days=0)), *list(data)) , con=cnxn)

    df['SCREENER'] = "TTM_SQZ"
    df['SCREENER_CRITERIA'] = "Lookback Days: " + str(lookback_period) + " Price Range: " + str(price_range) + " Volume: " + str(vol) 
    df['CREATED_DT'] = date.today()

    df.to_sql("daily_india_screener", con = mssql_engine, if_exists='append', index=False )

    msg = alerts.tg_msg_foramtter("TTM_SQZ" + "| Vol >" + str(vol) + "| Close >" + str(price_range), df[['SYMBOL','CLOSE_PRICE','CHANGE_PCT']])
    alerts.tg_send_msg(msg)

    return (df)

print (ttm_sqz(data))
print (pct4_chg(query.pct4_chg))

cnxn.close()