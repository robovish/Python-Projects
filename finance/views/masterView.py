import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import streamlit as st
import pandas as pd
from utils import alerts, db_conn, custom_functions
from data import query 
import pyodbc
from datetime import date, timedelta
from charts import chart
import toml
from plotly import graph_objects as go 
from plotly.subplots import make_subplots
import plotly.express as px

# config = toml.load(r'D:\Python-Projects\finance\config.toml')

st.set_page_config(page_title="FINANCIAL MARKET ANALYSIS", layout="wide" )

@st.cache(allow_output_mutation=True)
def get_connection():
    mssql_engine, cnxn, cursor = db_conn.db_conn()
    return (mssql_engine, cnxn, cursor)

mssql_engine, cnxn, cursor = get_connection()

@st.cache(hash_funcs={pyodbc.Connection : id})
def data_load():
    data = pd.read_sql('SELECT * FROM daily_stock_indicator_tmp', con = cnxn)
    return (data)

data = data_load()

st.sidebar.title('MARKET DASHBOARD')

dashboard = ['MARKET VIEW', 'DAILY SCREENER', 'SCREENER ANALYSIS', 'FUNDAMENTAL ANALYSIS', 'PORTFOLIO ANALYSIS']

dashboard_type = st.sidebar.selectbox("DASHBOARD", dashboard)
# print (dashboard_type)

if dashboard_type == 'MARKET VIEW':

    index_symbols = pd.read_sql("""SELECT DISTINCT SYMBOL FROM dbo.index_daily""", con= cnxn)
    market = st.sidebar.selectbox("MARKET", ['GLOBAL INDEX', 'INDIA INDEX'])
    if market == 'INDIA INDEX':
            choices = st.multiselect("CHOOSE INDEX", index_symbols)

            param_cnt = ','.join(['?']*len(choices))
            qry = """SELECT * FROM dbo.index_daily WHERE DATE >= '2010-01-01' AND SYMBOL IN ({}) ORDER BY SYMBOL, COUNTRY, DATE ASC""".format(param_cnt)
            data = pd.read_sql_query(qry, params= (choices) , con=cnxn)

            df =  data.sort_values(by=['SYMBOL', 'COUNTRY', 'DATE'], ascending=True, axis=0).groupby(['SYMBOL','COUNTRY']).apply(custom_functions.data_series_scale).reset_index(drop=True)
            df.rename('CLOSE_SCALED', inplace=True)
            data = pd.concat([data, df], axis=1, ignore_index=False)
            print (data)
            
            st.plotly_chart(chart.plotly_index_chart(data))

    

elif dashboard_type in 'DAILY SCREENER':

    # """Code for showing Screened Stocks"""
    screener = pd.read_sql("""SELECT DISTINCT SCREENER FROM daily_india_screener""", con = cnxn)
    screener_type = st.sidebar.selectbox("SCREENER", screener)
    price = st.sidebar.slider("PRICE", 0, 5000, step = 500)
    vol = st.sidebar.slider("VOLUME", 2000000, 50000000)

    params = (screener_type, price, vol)

    symbol = pd.read_sql(query.screener, params= params,  con = cnxn)
    symbol_selected = st.sidebar.selectbox("SYMBOL", symbol['SYMBOL'].values.tolist())
    print (symbol_selected)
    df = data.loc[data['SYMBOL']==symbol_selected]
    # df['SYMBOL'][0]
    st.plotly_chart(chart.plotly_stock_charts(df),  use_container_width = False, width=1300 , height=800)
    # df
    col1 , col2 = st.beta_columns(2)

    with col1:
        st.subheader('WATCHLIST WIDGET')

        with st.form(key = 'Watchlist'):
            symbol = st.text_input('SYMBOL', value = symbol_selected, max_chars = 20,)
            notes = st.text_area('NOTES', "", max_chars = 200, height = 10)
            created = date.today() 

            submitted = st.form_submit_button('Submit')

            if submitted:
                params = (symbol, notes, created)
                qry = """INSERT INTO watchlist_notes(SYMBOL, NOTES, CREATED_DT) VALUES (?,?,?)"""
                cursor.execute(qry, symbol, notes, created)    
                st.write("Submitted Successfully")
        
    
    with col2:
        st.subheader('WATCHLIST DETAILS')
        dt = st.date_input('DATE', value =None)
        df_wl = pd.read_sql_query("""SELECT * FROM watchlist_notes WHERE CREATED_DT > = ?""" , params = [dt], con=mssql_engine)
        st.dataframe(df_wl)

elif dashboard_type in 'SCREENER ANALYSIS':
    pass
    
