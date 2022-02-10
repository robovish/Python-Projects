import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import pandas as pd
from utils import db_conn

mssql_engine, cnxn, cursor = db_conn.db_conn()

def db_bckup():

    """
    Function: 
    This function is used to take backup of data on a Qtrly or Half yearly basis as a full dump and loading it in Local and Gdrive-celdsaa accnt

    Returns:
    None 

    Comments:
    Save the file in.csv format in local and then zip it and upload to Cloud. Everytime full backup needs to be taken in order to accomodate for
    data patches applied to stocks.
    Intraday Hist and tick tbl are not downloaded as it can be created on need basis once db is recreated.
    """
    
    path = r'D:\NSE_Data_Backup\\'

    main_data_tbl = ['india_stocks_monthly', 'india_stocks_weekly', 'india_stocks_daily', 'index_daily']

    other_tbl = ['master_index_list', 'master_india_stocks_list', 'master_india_stock_reference', 'master_world_stocks_list',
                'exclusion_inclusion_list', 'india_stock_category', 'india_symbol_update_tracker', 'master_india_screener', 'watchlist_notes']

    for tbl in main_data_tbl:
        qry = 'SELECT * FROM ' + tbl + ' WHERE DATE <=' + """'2021-12-31'"""
        print (qry)
        df = pd.read_sql(qry, con=cnxn)

        fname = 'Main\\' +tbl+'_2021-12-31.csv'
        df.to_csv(path+fname, index=False)

    for tbl in other_tbl:
        qry = 'SELECT * FROM ' + tbl
        print (qry)
        df = pd.read_sql(qry, con=cnxn)

        fname = 'Others\\' + tbl+ '_2021-12-31.csv'
        df.to_csv(path+fname, index=False)

# db_bckup()

cnxn.close()