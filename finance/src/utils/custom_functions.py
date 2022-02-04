import numpy as np
import pandas as pd 

def data_series_scale(data, val=[]):

    """
    Function: Used for scaling the data between 0 and 1. Used for Scaling various Indexes to same scale
    Input: Close price data
    Returns: List of values
    """

    val = (data['CLOSE']-data['CLOSE'].min())/(data['CLOSE'].max()-data['CLOSE'].min())
    val.append(val)
    val.rename('CLOSE_SCALED', inplace=True)
    # print (val)
    print (type(val) , "custom_fn")
    return (val)

def get_slope(array):

    """
    Function: Used for getting slope of any numeric series.
    Input: pandas Series, numpy 1D array
    Retuns: Slope of line
    """

    y = np.array(array)
    x = np.arange(len(y))
    slope, intercept, r_value, p_value, std_err = np.linregress(x,y)
    return slope

    

def selenium_driver(driver_path = None, file_download_path=None):

    from selenium import webdriver
    from selenium.webdriver.common.by import By
    
    chrome_options = webdriver.ChromeOptions() 
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # prefs = {"profile.default_content_settings.popups": 0,
    #      "download.default_directory": r"C:\Users\celds\Desktop\\"}
    # chrome_options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(driver_path, chrome_options=chrome_options)
    
    return (driver)


def stock_basket_price():
    """
    Function: To be run once for segregating stocks by Stock price category.
    
    Returns:
    A xlsx file with stocks list.
    
    Comment:
    Not needed any more as SQL script directly will be be doing the work.    
    """
    baskets = [(0,50), (50,500), (500,1000), (1000,2000), (2000,3000), (3000,5000), (5000,10000), (10000,100000)]

    with pd.ExcelWriter(r"""C:\Users\celds\Desktop\stocks_basket.xlsx""") as writer: 
        for i, params in enumerate(baskets):
            df = pd.read_sql(r"""SELECT SYMBOL, DATE, CLOSE_PRICE, VOLUME, CREATED_DT FROM india_stocks_daily isd 
                            WHERE CLOSE_PRICE BETWEEN (?) AND (?) AND CREATED_DT = CONVERT (DATE, GETDATE()-1)
                            ORDER BY CLOSE_PRICE""", 
                            params = params, con=cnxn)
            print (df)
            sheet_name = "Sheet_"+str(params[0]) + '_' + str(params[1])
            df.to_excel(writer, index=False, sheet_name = sheet_name )

