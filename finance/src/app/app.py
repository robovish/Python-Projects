import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import pandas as pd
import toml
import streamlit as st
# from charts import chart, markers
from utils import alerts, db_conn
from data import query
# import talib as pta
# import mplfinance 
import streamlit.components.v1 as components

config = toml.load(r'D:\Python-Projects\finance\config.toml')

mssql_engine, cnxn, cursor = db_conn.db_conn()

# data = indicator.data_process(indicator.data_load(cnxn))

# df = pd.read_sql("""SELECT TOP 10 SYMBOL, CLOSE_PRICE, CHANGE_PCT FROM india_stocks_daily WHERE  DATE = '2021-12-08'""", con =cnxn)
# print (data.head())

st.write('xyz')

components.iframe("""
<iframe height="800" width="1400" src="https://sslcharts.investing.com/index.php?force_lang=1&pair_ID=1&timescale=300&candles=100&style=candles"></iframe><br /><div style="width:500px"><a target="_blank" href="https://www.investing.com"><img src="https://wmt-invdn-com.investing.com/forexpros_en_logo.png" alt="Investing.com" title="Investing.com" style="float:left" border="0"/></a><span style="float:right"><span style="font-size: 11px;color: #333333;text-decoration: none;">Forex Charts powered by <a href="https://www.investing.com/" rel="nofollow" target="_blank" style="font-size: 11px;color: #06529D; font-weight: bold;" class="underline_link">Investing.com</a></span></span></div>
""")
cnxn.close()
