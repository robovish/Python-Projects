import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from alice_blue import *
import  time
import toml 
from datetime import date, timedelta, datetime, time
from utils import api_conn, db_conn

mssql_engine, cnxn, cursor = db_conn.db_conn()

config = toml.load(r'D:\Python-Projects\finance\config.toml')

user_id = config['BROKER_ALICEBLUE']['USER_ID']
password = config['BROKER_ALICEBLUE']['PASSWORD']
twoFA = config['BROKER_ALICEBLUE']['TWOFA']
app_id = config['BROKER_ALICEBLUE']['APP_ID']
api_secret = config['BROKER_ALICEBLUE']['APP_SECRET']
log_path_ab = config['BROKER_ALICEBLUE']['LOG_PATH']

alice_session, access_token = api_conn.aliceblue_api_session(user_id, password, twoFA, app_id, api_secret)

# with open("""D:\\Python-Projects\\finance\\data\\access_token_aliceblue.txt""", 'r') as fl:
#         access_token = fl.read()
#         print (access_token)

# print(alice_session.get_profile())

socket_opened = False
symbols = ['SBIN', 'ONGC'] # This needs to be taken from DB table for Intraday stocks list
instruement_list = []

for val in symbols:
    instruement_list.append(alice_session.get_instrument_by_symbol('NSE', val))

print (instruement_list)


def event_handler_quote_update(msg):
    print(msg)
    
    SYMBOL = msg['instrument'].symbol
    DATE = (datetime.fromtimestamp(msg['ltt']) + timedelta(hours=10.5)).strftime('%Y-%m-%d %H:%M:%S')
    LTP = msg['ltp']
    OPEN_PRICE = msg['open']
    HIGH_PRICE = msg['high']
    LOW_PRICE = msg['low']
    CLOSE_PRICE = msg['close']
    VOLUME = msg['volume']
    LTQ = msg['ltq']
    TOTAL_BID_QTY =msg['total_buy_quantity']
    TOTAL_ASK_QTY = msg['total_sell_quantity']
    
    values = (SYMBOL, DATE, LTP, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME, LTQ, TOTAL_BID_QTY, TOTAL_ASK_QTY)
    print (values)
    
    qry = """INSERT INTO intraday_tick_data1
    (SYMBOL, DATE, LTP, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME, LTQ, TOTAL_BID_QTY, TOTAL_ASK_QTY)
    VALUES""" + str(values)
    print (qry)
    cursor.execute(qry)
    
def open_callback():
    global socket_opened
    socket_opened = True

alice_session.start_websocket(subscribe_callback=event_handler_quote_update, socket_open_callback=open_callback, run_in_background=False)

while True:
    # Currently working in EST TZ. Needs to be changed to IST.
    if ((datetime.now().time() >= time(22,45,0)) and (datetime.combine(date.today(), time(22,45,0)) + timedelta(hours=6))): 
        alice_session.subscribe(instruement_list, LiveFeedType.MARKET_DATA)
        
        
cnxn.close()