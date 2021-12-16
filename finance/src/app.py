import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import pandas as pd
import toml
import streamlit as st
# from charts import chart, markers
from utils import alerts, db_conn
from data import query
# import talib as pta
import mplfinance 

config = toml.load(r'D:\Python-Projects\finance\config.toml')

mssql_engine, cnxn, cursor = db_conn.db_conn()

# data = indicator.data_process(indicator.data_load(cnxn))

# df = pd.read_sql("""SELECT TOP 10 SYMBOL, CLOSE_PRICE, CHANGE_PCT FROM india_stocks_daily WHERE  DATE = '2021-12-08'""", con =cnxn)
# print (data.head())

st.write('xyz')

cnxn.close()
