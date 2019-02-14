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

connection = pymysql.connect(host='127.0.0.1', user='', password='', db='',
                                 charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, connect_timeout=31536000)
cursor = connection.cursor()


# Declare
start = datetime.now()
args_list = []
count = str(250)
myFormat = "%Y-%m-%d"
today = datetime.now()
start_date = today.strftime(myFormat)
equity_list = []

# /* Drop the table */
drop = "truncate table daily_quotes;"
cursor.execute(drop)
print(cursor._last_executed)


query = "select * from list_of_equities_tracked"
cursor.execute(query)

result_set = cursor.fetchall()
for row in result_set:
    stock = row['equity_name']
    stock = option_library.clean_stock('td',stock)
    print(stock)
    time.sleep(2)
    url = 'https://api.tdameritrade.com/v1/marketdata/quotes?apikey=KEY' +stock
    r = requests.get(url)
    payload = r.json()
    myFormat = "%Y-%m-%d"
    today = datetime.now()
    Date_daily_quotes = today.strftime(myFormat)
    print(payload)
    for key, value in payload.items():
        closePrice = value['regularMarketLastPrice'] if stock != "$SPX.X" else value['lastPrice']
        stock = stock if stock != "$SPX.X" else 'spx'
        # if stock == "$SPX.X":
        #     closePrice = value['lastPrice']
        # else:
        #     closePrice = value['regularMarketLastPrice']

        args = [value['openPrice'], value['highPrice'], value['lowPrice'], closePrice,
                Date_daily_quotes, value['totalVolume'], stock, value['description']]
        args_list.append(args)

q = "insert into daily_quotes (`Open`,High,Low,Close_price,Date_daily_quotes,Volume,equity,Name) values (%s, %s, %s, %s, %s, %s, %s, %s); "
cursor.executemany(q, args_list)
connection.commit()
print(cursor._last_executed)
