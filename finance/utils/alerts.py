import requests
import toml
from datetime import datetime, date   

config = toml.load(r'D:\Python-Projects\finance\config.toml')
print()


def tg_msg_foramtter(header, data):
    dt_today = date.today().strftime("%d/%m/%Y")
    fmt_data = dt_today  + '\n' + header + '\n' +  str(data)
    return (fmt_data)


def tg_send_msg(msg):

    """
    Function: To send message on Telegram    
    """

    BASE_URL = config['TELEGRAM']['BASE_URL']
    BOT_TOKEN = config['TELEGRAM']['BOT_TOKEN']
    BOT_CHATID = config['TELEGRAM']['BOT_CHATID']
    DISABLE_NOTIFICATION = config['TELEGRAM']['DISABLE_NOTIFICATION']

    uri = BASE_URL + BOT_TOKEN +  "/sendMessage"
    # r'https://api.telegram.org/bot5031931743:AAF6IDFgQ7e36mN3DEpNSIdQfSzu8eflrvs/'
    
    parameters = {
    "chat_id" : BOT_CHATID, 
    "text" : msg,
    "parse_mode" : "HTML", #MarkdownV2
    "disable_notification" : DISABLE_NOTIFICATION
    }

    resp = requests.get(uri, data = parameters)
    return (resp.text)


# tg_send_msg("Another msg from VCS1")