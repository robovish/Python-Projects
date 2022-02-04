import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from fyers_api import accessToken
import time
import toml 
from utils.custom_functions import selenium_driver
from selenium import webdriver
from selenium.webdriver.common.by import By
import re
from alice_blue import *

config = toml.load(r'D:\Python-Projects\finance\config.toml')

client_id = config['BROKER_FYERS']['CLIENT_ID']
redirect_uri = config['BROKER_FYERS']['REDIRECT_URI']
secret_key = config['BROKER_FYERS']['SECRET_KEY']
grant_type = config['BROKER_FYERS']['GRANT_TYPE']
response_type = config['BROKER_FYERS']['RESPONSE_TYPE']
state = config['BROKER_FYERS']['STATE']
log_path_fyers = config['BROKER_FYERS']['LOG_PATH']
driver_path = config['WEB_AUTOMATION']['SELENIUM_DRIVER_PATH']

user_id = config['BROKER_ALICEBLUE']['USER_ID']
password = config['BROKER_ALICEBLUE']['PASSWORD']
twoFA = config['BROKER_ALICEBLUE']['TWOFA']
app_id = config['BROKER_ALICEBLUE']['APP_ID']
api_secret = config['BROKER_ALICEBLUE']['APP_SECRET']
log_path_ab = config['BROKER_ALICEBLUE']['LOG_PATH']


def fyers_api_session(client_id, redirect_uri, secret_key, grant_type, response_type, state):

    fyers_session= accessToken.SessionModel(client_id=client_id, redirect_uri=redirect_uri, secret_key = secret_key, 
                                  grant_type= grant_type, response_type = response_type)

    session_url = fyers_session.generate_authcode()

    driver = selenium_driver(driver_path)
    driver.get(session_url)

    driver.find_element(By.ID , "fy_client_id").send_keys('XM00539')
    driver.find_element(By.ID ,'clientIdSubmit').click()
    time.sleep(2)

    #-----Code to enter Password---------
    driver.find_element(By.ID , "fy_client_pwd").send_keys('Omdpv@12041986')
    driver.find_element(By.ID ,'loginSubmit').click()
    time.sleep(2)

    # --- Code to enter Pin------
    driver.find_element(By.XPATH , '/html/body/section[8]/div[3]/div[3]/form/div[2]/input[1]').send_keys(1)
    driver.find_element(By.XPATH , '/html/body/section[8]/div[3]/div[3]/form/div[2]/input[2]').send_keys(9)
    driver.find_element(By.XPATH ,'/html/body/section[8]/div[3]/div[3]/form/div[2]/input[3]').send_keys(5)
    driver.find_element(By.XPATH ,'/html/body/section[8]/div[3]/div[3]/form/div[2]/input[4]').send_keys(3)
    driver.find_element(By.ID , 'verifyPinSubmit').click()

    time.sleep(5)

    url_with_token = driver.current_url

    auth_code = re.search(r'(auth_code=)(.*)(&state=None)', url_with_token).group(2)

    fyers_session.set_token(auth_code)
    response = fyers_session.generate_token()
    access_token = response["access_token"]

    with open(r'D:\Python-Projects\finance\data\access_token_fyers.txt', 'w') as fl:
                fl.write(access_token)

    return (fyers_session, access_token)



def aliceblue_api_session(user_id, password, twoFA, app_id, api_secret):

    access_token = AliceBlue.login_and_get_access_token(username= user_id, password= password, twoFA= twoFA, 
                                                        app_id=app_id, api_secret= api_secret)

    with open(r'D:\Python-Projects\finance\data\access_token_aliceblue.txt', 'w') as fl:
                fl.write(access_token)
    alice_session = AliceBlue(username=user_id, password=password, access_token=access_token, master_contracts_to_download=['NSE'])

    print (alice_session, access_token)

    return (alice_session, access_token)


# session, access_token = fyers_api_session(client_id, redirect_uri, secret_key, grant_type, response_type, state)

# session, access_token = aliceblue_api_session(user_id, password, twoFA, app_id, api_secret)
