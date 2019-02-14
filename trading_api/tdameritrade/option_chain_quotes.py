from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs
import requests
import ssl
import sys

from sys import argv
import option_library
import pymysql.cursors
from datetime import datetime
import time
import math

# Arguements
script, equity = argv


# Declare
start = datetime.now()
args_list = []
count = str(250)
myFormat = "%Y-%m-%d"
today = datetime.now()
start_date = today.strftime(myFormat)
equity_list = []

# for stock in equity_list:
start_equity = datetime.now()
print(equity)
url = 'https://api.tdameritrade.com/v1/marketdata/chains?apikey=KEY' + \
    equity + '&strikeCount=' + count + '&fromDate=' + \
    start_date + '&toDate=2021-12-31'
print(url)
r = requests.get(url)
payload = r.json()

table = equity + str("_options")
equity = payload["symbol"]
option_library.build_table(table)

# Get Puts
for keyy, valuee in payload["putExpDateMap"].items():
    d = datetime.strptime(keyy, "%Y-%m-%d:%f")
    ex_date = d.strftime(myFormat)
    for key, value in valuee.items():
        for v in value:
            IBsymbol = option_library.build_IB_symobl(d, v['putCall'], v['strikePrice'], equity)

            args = [v['strikePrice'], v['symbol'], v['last'], v['bid'], v['ask'], v['markChange'], v['percentChange'], v['totalVolume'], v['openInterest'], v['volatility'], v['putCall'], payload["symbol"],  ex_date, v['theoreticalOptionValue'], v['theoreticalOptionValue'],  v['inTheMoney'],   v['delta'], v['gamma'], v['theta'], v['vega'], v['rho'], v['daysToExpiration'], v['timeValue'], v['theoreticalVolatility'],
                    v['mark'], v['bidSize'], v['askSize'], IBsymbol]
            args_list.append(args)
            # print(args_list)
    # sys.exit()
# Get Calls
for keyy, valuee in payload["callExpDateMap"].items():
    d = datetime.strptime(keyy, "%Y-%m-%d:%f")
    ex_date = d.strftime(myFormat)
    for key, value in valuee.items():
        for v in value:
            IBsymbol = option_library.build_IB_symobl(
                d, v['putCall'], v['strikePrice'], equity)

            args = [v['strikePrice'], v['symbol'], v['last'], v['bid'], v['ask'], v['markChange'], v['percentChange'], v['totalVolume'], v['openInterest'], v['volatility'], v['putCall'], payload["symbol"],  ex_date, v['theoreticalOptionValue'], v['theoreticalOptionValue'],  v['inTheMoney'],   v['delta'], v['gamma'], v['theta'], v['vega'], v['rho'], v['daysToExpiration'], v['timeValue'], v['theoreticalVolatility'],
                    v['mark'], v['askSize'], v['bidSize'], IBsymbol]
            args_list.append(args)

connection = pymysql.connect(host='127.0.0.1', user='', password='', db='optionsTD',
                             charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, connect_timeout=31536000)
cursor = connection.cursor()
q = "INSERT INTO " + table + "( `strike`, `symbol`, `last`, `bid`, `ask`, `change`, `change_p`, `volume`, `open_interest`, `TD_IVOL`,  `opt_type`, `equity`, `ex_date`, `call_price_intrinsic`, `put_price_intrinsic`,   `ITM`,   `deltaTD`, `gammaTD`, `thetaTD`, `vegaTD`, `rhoTD`, `days_to_expiration`,time_value,theoretical_volatility,mark, ask_size,bid_size,IBsymbol) VALUES (   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,  %s, %s, %s,%s, %s,%s,%s, %s, %s); "
cursor.executemany(q, args_list)
connection.commit()
connection.close()


finish_equity = datetime.now()
print(finish_equity - start_equity)
