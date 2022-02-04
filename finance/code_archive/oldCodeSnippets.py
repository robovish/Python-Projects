

# def indicators(data):
#     """
#     Function: 
#     To apply various indicators to the data used for building multiple Screening criteria    
#     """

#     ttm_squeeze = pta.squeeze(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], bb_length= 21, bb_std=2, 
#                               kc_length=21, kc_scalar=1.5,detailed=False, mamode= 'ema' )
    
#     bbands = pta.bbands(data['CLOSE_PRICE'], length=21, std=2, talib=True, mamode='ema')
    
#     keltner_channel = pta.kc(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], length=21, 
#                              scalar=1.5, mamode='ema', offset=None)
    
#     adx = pta.adx(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], length=21)    
    
#     atr = pta.atr(data['HIGH_PRICE'], data['LOW_PRICE'], data['CLOSE_PRICE'], length=21, mamode= 'ema')
    
#     df = pd.concat([data, ttm_squeeze, bbands, keltner_channel,adx], axis= 1)
    
#     df.set_index(pd.DatetimeIndex(df['DATE']), inplace=True)

#     df.rename(columns={"OPEN_PRICE": "Open", "HIGH_PRICE": "High", "LOW_PRICE": "Low", "CLOSE_PRICE": "Close", "VOLUME": "Volume" },
#          inplace=True)
    
#     return df

def ttm_sqz(data, lookback_period=5, price_range=50, vol=100000):

    dt = date.today() - timedelta(days=5)
    # data = data.loc[(data['DATE'] >= date(dt.year,dt.month,dt.day)) & (data['SQZ_ON']==1) & (data.iloc[-1,5] > price_range) & (data.iloc[-1,8] > vol)]['SYMBOL'].unique()

    data = data.loc[(data['DATE'] >= date(dt.year,dt.month,dt.day)) & (data['SQZ_ON']==1) & (data['Close'] > price_range) & (data['Volume'] > vol)]['SYMBOL'].unique()

    param_cnt = ','.join(['?']*len(data))
    qry = """SELECT SYMBOL, DATE, CLOSE_PRICE, VOLUME, CHANGE_PCT FROM india_stocks_daily WHERE DATE = ? AND SYMBOL IN ({}) """.format(param_cnt)
    df = pd.read_sql_query(qry, params= ((date.today() - timedelta(days=0)), *list(data)) , con=cnxn)

    df['SCREENER'] = "TTM_SQZ"
    df['SCREENER_CRITERIA'] = "Lookback Days: " + str(lookback_period) + " Price Range: " + str(price_range) + " Volume: " + str(vol) 
    df['CREATED_DT'] = date.today()

    df.to_sql("master_india_screener", con = mssql_engine, if_exists='append', index=False )

    msg = alerts.tg_msg_foramtter("TTM_SQZ" + "| Vol >" + str(vol) + "| Close >" + str(price_range), df[['SYMBOL','CLOSE_PRICE','CHANGE_PCT']])
    alerts.tg_send_msg(msg)

    return (df)