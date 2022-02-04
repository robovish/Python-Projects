import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import pandas as pd
from datetime import date, timedelta
import time
import os
from utils import custom_functions, db_conn
import toml 
from sqlalchemy.types import Date
import investpy as ip

config = toml.load(r'D:\Python-Projects\finance\config.toml')

mssql_engine, cnxn, cursor = db_conn.db_conn()

url_list = config['NSE']['URL_LIST']
index_stock_url_list = config['NSE']['INDEX_STOCK_URL_LIST']
table_list = config['NSE']['TABLE_LIST']
selenium_driver_path = config['WEB_AUTOMATION']['SELENIUM_DRIVER_PATH']
file_download_path = config['WEB_AUTOMATION']['SELENIUM_FILE_DOWNLOAD_PATH']
nse_holidays = config['NSE']['NSE_HOLIDAYS']

# print (url_list, selenium_driver_path, file_download_path )

def nse_data_download(url_list, driver_path, file_download_path=None):
    
    driver = custom_functions.selenium_driver(driver_path)
    
    for val in url_list:
        print (val)
        driver.get(val)
        time.sleep(3)


def master_stock_db_update(path =None):
    
    path = r"""C:\Users\celds\Downloads\\"""
    print ('Starting master_stock_db_update')
    fname = path +  'EQUITY_L.csv'
    # print (fname)
    
    dt = (date.today() - timedelta(0)).strftime("%Y-%m-%d")
    
    df_nse_stocks = pd.read_csv(fname, parse_dates=[' DATE OF LISTING'])
    
    col_new = ['SYMBOL','COMPANY_NAME','SERIES','DATE_OF_LISTING','PAID_UP_VALUE','MARKET_LOT','FACE_VALUE','ISIN','CREATED_DT']

    df_nse_stocks['CREATED_DT'] = date.today()

    df_nse_stocks = df_nse_stocks[['SYMBOL', 'NAME OF COMPANY', ' SERIES', ' DATE OF LISTING',' PAID UP VALUE', ' MARKET LOT', 
                                   ' FACE VALUE', ' ISIN NUMBER',  'CREATED_DT']]

    df_nse_stocks.columns = col_new
    
    df_nse_stocks.to_sql(name= 'master_india_stocks_list', con= mssql_engine, if_exists='replace', index=False,  dtype = {'DATE_OF_LISTING' : Date})


    #-------------- Below code will update india_symbol_update_tracker tbl------------------
    df1 = df_nse_stocks.loc[(df_nse_stocks['DATE_OF_LISTING'] == dt)][['SYMBOL', 'ISIN', 'DATE_OF_LISTING']]
    if not(df1.empty):
        df1['UPDATE_TYPE'] = 'NEW_LISITNG'
        df1['CREATED_DT'] = date.today()
        df1['NEW_SYMBOL'] = df1['SYMBOL']
        df1.rename({'SYMBOL': 'OLD_SYMBOL', 'DATE_OF_LISTING': 'UPDATE_DT'}, inplace=True, axis=1)
        df1 = df1[['UPDATE_TYPE', 'OLD_SYMBOL', 'NEW_SYMBOL', 'ISIN', 'UPDATE_DT', 'CREATED_DT']]
        df1.to_sql(name= 'india_symbol_update_tracker', con= mssql_engine, if_exists='append', index=False,  dtype = {'UPDATE_DT' : Date})
    else:
        print('No Symbol updates found')
        
    os.remove(fname)

    return (None)


def symbol_chg_update(table_list, path = None):
    
    path = r"""C:\Users\celds\Downloads\\"""
    fname = path +  'symbolchange.csv'
    print (fname)
    
    dt = (date.today() - timedelta(0)).strftime("%Y-%m-%d")

    df_symbol_chg = pd.read_csv(fname, encoding = 'latin1', parse_dates=[' SM_APPLICABLE_FROM'])
    df_symbol_chg.columns = ['COMPANY_NAME', 'OLD_SYMBOL', 'NEW_SYMBOL', 'CHANGE_DATE']
    
    df1 = df_symbol_chg.loc[(df_symbol_chg['CHANGE_DATE'] == dt) & (df_symbol_chg['OLD_SYMBOL'].notnull())]
    print (df1['OLD_SYMBOL'].to_list())
    
    if not(df1.empty):
        for tbl in table_list:
            print (tbl)
            for i,val in df1.iterrows():
                print (val['OLD_SYMBOL'])
                sql = "UPDATE " + tbl + """ SET SYMBOL= (?) WHERE SYMBOL = (?) """
                cursor.execute(sql, (val['NEW_SYMBOL'], val['OLD_SYMBOL']) )

#-----------Below code will insert symbol which has been change in india_symbol_update_tracker tbl-------

        df2 = df_symbol_chg.loc[(df_symbol_chg['CHANGE_DATE'] == dt)][['OLD_SYMBOL', 'NEW_SYMBOL', 'CHANGE_DATE']]
        df2['UPDATE_TYPE'] = 'SYMBOL_CHANGE'
        df2['CREATED_DT'] = date.today()
        df2['ISIN'] = ''
        df2.rename({'CHANGE_DATE': 'UPDATE_DT'}, inplace=True, axis=1)
        df2 = df2[['UPDATE_TYPE', 'OLD_SYMBOL', 'NEW_SYMBOL', 'ISIN', 'UPDATE_DT', 'CREATED_DT']]
        df2.to_sql(name= 'india_symbol_update_tracker', con= mssql_engine, if_exists='append', 
                    index=False,  dtype = {'UPDATE_DT' : Date})
        # print (df2)
    else:
        print('No Symbol updates found')
    
    os.remove(fname)

    return (None)



def delisted_symbol_update(table_list, path = None):
    
    path = r"""C:\Users\celds\Downloads\\"""
    fname = path +  'delisted.xlsx'
    
    dt = (date.today() - timedelta(0)).strftime("%Y-%m-%d")

    df_symbol_chg = pd.read_excel(fname, usecols=['Symbol', 'Delisted Date'])
    df_symbol_chg.columns = ['SYMBOL', 'DELIST_DATE']
    
    df1 = df_symbol_chg.loc[(df_symbol_chg['DELIST_DATE'] == dt)]
    
    symbol_list = df1['SYMBOL'].to_list()
    param_cnt = ','.join(['?']*len(symbol_list))
    
    if not(df1.empty):
        print (symbol_list)
        for tbl in table_list:
            sql = "DELETE FROM " + tbl + """ WHERE SYMBOL IN ({}) """.format(param_cnt)
            print (sql)
            cursor.execute(sql, (symbol_list))

#-----------Below code will insert symbol which has been delisted in india_symbol_update_tracker tbl-------
        df1['UPDATE_TYPE'] = 'DELISTED'
        df1['CREATED_DT'] = date.today()
        df1['ISIN'] = ''
        df1['NEW_SYMBOL'] = df1['SYMBOL']
        df1.rename({'DELIST_DATE': 'UPDATE_DT', 'SYMBOL': 'OLD_SYMBOL'}, inplace=True, axis=1)
        df2 = df1[['UPDATE_TYPE', 'OLD_SYMBOL', 'NEW_SYMBOL', 'ISIN', 'UPDATE_DT', 'CREATED_DT']]
        df2.to_sql(name= 'india_symbol_update_tracker', con= mssql_engine, if_exists='append', index=False,  dtype = {'UPDATE_DT' : Date})
    else:
        print('No Symbol Delisted')
        
    os.remove(fname)   
    
    return (None)




def stock_symbol_update_investpy():
    
    '''Function:
    This function is used to update stock.csv of Investpy package. It lookup from master stock symbol tbl for India and then
    search in Investpy API to get corresponding symbol in Investing.com.
    Manually this file needs to be copied into stock.csv in below path:
    C:\\Users\\celds\\Anaconda3\\envs\\finance\\Lib\\site-packages\\investpy\\resources
    
    Comments:
    Needs to be run daily to update the stock.csv in investpy pkg.
    **Delisted stocks needs to be manually removed for now based on symbol_tracker tbl
    '''
    
    stock_not_found = []
    
    df_ip = pd.read_csv(r"""C:\Users\celds\Anaconda3\envs\finance\Lib\site-packages\investpy\resources\stocks.csv""")
    
    stock_list = pd.read_sql("""SELECT t1.UPDATE_TYPE, t1.OLD_SYMBOL , t1.NEW_SYMBOL, t2.ISIN, t2.COMPANY_NAME , t1.UPDATE_DT , t1.CREATED_DT 
                             FROM india_symbol_update_tracker t1 
                             LEFT JOIN master_india_stocks_list t2 
                             ON t1.NEW_SYMBOL = t2.SYMBOL 
                             WHERE t1.CREATED_DT >= CONVERT(DATE,DATEADD(DAY , -5, GETDATE()))""",  con=cnxn)

    df_stock_list = pd.DataFrame(columns = ['country', 'name', 'full_name', 'tag', 'isin', 'id', 'currency', 'symbol'])
    
    if not(stock_list.empty):
        for i, val in stock_list.iterrows():
            try:
                search_result = ip.search_quotes(text=val[3], products=['stocks'], countries=['india'], n_results=1)
                if ((val[0] == 'NEW_LISITNG')):
                    print (val[2], val[3])
                    df_stock_list.loc[i,'country'] = search_result.country
                    df_stock_list.loc[i,'name'] = search_result.name
                    df_stock_list.loc[i,'full_name'] = search_result.name
                    df_stock_list.loc[i,'tag'] = search_result.tag
                    df_stock_list.loc[i,'isin'] = val[3]
                    df_stock_list.loc[i,'id'] = search_result.id_
                    df_stock_list.loc[i,'currency'] = "INR"
                    df_stock_list.loc[i,'symbol'] = search_result.symbol
                    if (val[3] not in df_ip['isin'].to_list()):
                        df_ip = df_ip.append(df_stock_list, ignore_index=True)
                        print ('Symbol added', val[2])
                    else:
                        print ('Symbol arlready present')

                elif(val[0] == 'SYMBOL_CHANGE'):
                    if (val[3] in df_ip['isin'].to_list()):
                        idx = df_ip[df_ip['isin'] == val[3]].index[0]
                        df_ip.loc[idx, 'country'] = search_result.country
                        df_ip.loc[idx, 'name'] = search_result.name
                        df_ip.loc[idx, 'full_name'] = search_result.name
                        df_ip.loc[idx, 'tag'] = search_result.tag
                        df_ip.loc[idx, 'isin'] = val[3]
                        df_ip.loc[idx, 'id'] = search_result.id_
                        df_ip.loc[idx, 'currency'] = 'INR'
                        df_ip.loc[idx, 'symbol'] = search_result.symbol
                    else:
                        print ('Symbol to be updated is not found', val[3])
            except:
                stock_not_found.append(val[1])
                print ('Stock not found', val[1])
                continue
    else:
        print("No Stock Updates in Investpy")

    df_ip.to_csv(r"""C:\Users\celds\Anaconda3\envs\finance\Lib\site-packages\investpy\resources\stocks.csv""", index=False)

    return (None)


def index_stock_symbol_mapping():

    nse_data_download(index_stock_url_list, selenium_driver_path, file_download_path)

    fname = {'NIFTY 50' :'ind_nifty50list.csv', 'NIFTY 100' :'ind_nifty100list.csv','NIFTY 200':'ind_nifty200list.csv',
             'NIFTY 500':'ind_nifty500list.csv', 'NIFTY NEXT 50': 'ind_niftynext50list.csv',
             'NIFTY MIDCAP 150' : 'ind_niftymidcap150list.csv', 'NIFTY SMALLCAP 250': 'ind_niftysmallcap250list.csv',
             'NIFTY MICROCAP 250' : 'ind_niftymicrocap250_list.csv', 'NIFTY LARGEMIDCAP 250' : 'ind_niftylargemidcap250list.csv',
             'NIFTY MIDSMALLCAP 400' : 'ind_niftymidsmallcap400list.csv'}
    
    df_symbol = pd.DataFrame()
    
    # path = r'C:\Users\celds\Downloads\\'
    
    for index, fname in fname.items():

        df = pd.read_csv(file_download_path+fname)
        df['INDEX_NAME'] = index
        df['CREATED_DT'] = date.today()

        df = df[['INDEX_NAME', 'Symbol', 'Company Name', 'Industry','ISIN Code', 'Series', 'CREATED_DT']]
        df.columns = [',INDEX_NAME','SYMBOL','COMPANY_NAME', 'INDUSTRY', 'ISIN','SERIES','CREATED_DT']

        df_symbol = pd.concat([df, df_symbol], axis=0)
        # print (df_symbol)
        
        os.remove(path+fname)

    df_symbol.to_sql('index_symbol_mapping', con= mssql_engine, if_exists='replace', index=False)

    return (None)




# nse_data_download(url_list, selenium_driver_path, file_download_path)

# master_stock_db_update(file_download_path)

# symbol_chg_update(table_list, file_download_path)

# delisted_symbol_update(table_list, file_download_path)

# stock_symbol_update_investpy()

# index_stock_symbol_mapping()

if (str(date.today() + timedelta(0)) not in nse_holidays):
    if ((date.today() - timedelta(0)).day == 1):
        nse_data_download(url_list, selenium_driver_path, file_download_path)

        master_stock_db_update(file_download_path)

        symbol_chg_update(table_list, file_download_path)

        delisted_symbol_update(table_list, file_download_path)

        stock_symbol_update_investpy()

    elif ((date.today() - timedelta(0)).strftime("%a") not in ['Sun', 'Sat']):
        nse_data_download(url_list, selenium_driver_path, file_download_path)

        master_stock_db_update(file_download_path)

        symbol_chg_update(table_list, file_download_path)

        delisted_symbol_update(table_list, file_download_path)

        stock_symbol_update_investpy()
    else:
        print ('Today is a Weekend')
else:
    print ('Today is NSE Holiday')



cnxn.close()