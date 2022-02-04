import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from fyers_api.Websocket import ws
import  time
import toml 
from datetime import date, timedelta, datetime, time
from utils import api_conn, db_conn

mssql_engine, cnxn, cursor = db_conn.db_conn()

config = toml.load(r'D:\Python-Projects\finance\config.toml')

client_id = config['BROKER_FYERS']['CLIENT_ID']
redirect_uri = config['BROKER_FYERS']['REDIRECT_URI']
secret_key = config['BROKER_FYERS']['SECRET_KEY']
grant_type = config['BROKER_FYERS']['GRANT_TYPE']
response_type = config['BROKER_FYERS']['RESPONSE_TYPE']
state = config['BROKER_FYERS']['STATE']
log_path = config['BROKER_FYERS']['LOG_PATH']

# session, access_token = api_conn.fyers_api_session(client_id, redirect_uri, secret_key, grant_type, response_type, state)

# print (session, access_token)

with open("""D:\\Python-Projects\\finance\\data\\access_token_fyers.txt""", 'r') as fl:
        access_token = fl.read()
        print (access_token)

feed_token = client_id + ':' + access_token
data_type = "symbolData"
symbol = ["NSE:SBIN-EQ","NSE:ONGC-EQ"]  


def custom_message(msg):
    # print (msg)  
    SYMBOL = msg[0]['symbol']
    DATE = (datetime.fromtimestamp(msg[0]['timestamp'])  + timedelta(hours=10.5)) .strftime('%Y-%m-%d %H:%M:%S')
    LTP = msg[0]['ltp']
    OPEN_PRICE = msg[0]['open_price']
    HIGH_PRICE = msg[0]['high_price']
    LOW_PRICE = msg[0]['low_price']
    CLOSE_PRICE = msg[0]['close_price']
    VOLUME = msg[0]['vol_traded_today']
    LTQ = msg[0]['last_traded_qty']
    TOTAL_BID_QTY = msg[0]['tot_buy_qty']
    TOTAL_ASK_QTY = msg[0]['tot_sell_qty']

    values = (SYMBOL, DATE, LTP, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME, LTQ, TOTAL_BID_QTY,TOTAL_ASK_QTY)
    print (values)

    qry = """INSERT INTO intraday_tick_data
     (SYMBOL, DATE, LTP, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME, LTQ, TOTAL_BID_QTY,TOTAL_ASK_QTY)
     VALUES""" + str(values)
    # print (qry)
    cursor.execute(qry)


fs = ws.FyersSocket(access_token=feed_token,run_background=False,log_path=log_path)
fs.websocket_data = custom_message
fs.subscribe(symbol=symbol,data_type=data_type)
fs.keep_running()

# try:
#     while True:
#         if ((datetime.now().time() >= time(22,45,0)) and (datetime.combine(date.today(), time(22,45,0)) + timedelta(hours=6))): 
#             print ('yes')
#             # run_process_foreground_symbol_data(client_id, access_token)
#             fs.subscribe(symbol=symbol,data_type=data_type)
#             fs.keep_running()
#             print ('Started')
# except KeyboardInterrupt:
#     print ('Some issue')


# def run_process_foreground_symbol_data(client_id, access_token):
#     '''This function is used for running the symbolData in foreground 
#     1. log_path here is configurable this specifies where the output will be stored for you
#     2. data_type == symbolData this specfies while using this function you will be able to connect to symbolwebsocket to get the symbolData
#     3. run_background = False specifies that the process will be running in foreground'''

#     feed_token = client_id + ':' + access_token
#     data_type = "symbolData"
#     symbol = ["NSE:SBIN-EQ","NSE:ONGC-EQ"]   ##NSE,BSE sample symbols
#     # symbol =["NSE:NIFTY50-INDEX","NSE:NIFTYBANK-INDEX","NSE:SBIN-EQ","NSE:HDFC-EQ","NSE:IOC-EQ"]
#     # symbol =["MCX:SILVERMIC21NOVFUT","MCX:GOLDPETAL21SEPTFUT"]
#     fs = ws.FyersSocket(access_token=feed_token,run_background=False,log_path=log_path)
#     fs.websocket_data = custom_message

#     fs.subscribe(symbol=symbol,data_type=data_type)

#     fs.keep_running()

cnxn.close()