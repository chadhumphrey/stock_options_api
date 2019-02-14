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
table = equity + str("_rawdata")


# Declare
# start = datetime.now()
# args_list = []
# count = str(250)
myFormat = "%Y-%m-%d"
# today = datetime.now()
# start_date = today.strftime(myFormat)
# equity_list = []
# clean_up_date = "1999-01-01"
clean_up_date = "2013-01-01"
args_list = []
equity = option_library.clean_stock('td',equity)
print("boom {}".format(equity))
endDate = int(time.time())*1000
endDate = str(endDate)
print("boom {}".format(endDate))

# for stock in equity_list:
start_equity = datetime.now()
url = 'https://api.tdameritrade.com/v1/marketdata/'+equity.upper()+'/pricehistory?apikey=KEY&periodType=month&frequencyType=daily&startDate=852076800&endDate='+str(endDate)+''
print(url)
r = requests.get(url)
payload = r.json()


for v in payload['candles']:
    close_date= datetime.utcfromtimestamp(v['datetime']/1000).strftime(myFormat)
    close_date= datetime.strptime(close_date, myFormat)
    # print("Date :{}".format(close_date))
    # print("close :{}".format(v['close']))
    # print("open :{}".format(v['open']))
    # print("high :{}".format(v['high']))
    # print("low :{}".format(v['low']))
    # print("volume :{}".format(v['volume']))

    args = [close_date, v['close'], v['open'], v['high'], v['low'], v['volume']]
    args_list.append(args)
print(args_list)
connection = pymysql.connect(host='127.0.0.1', user='', password='', db='DEV_stocks',charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, connect_timeout=31536000, autocommit=True)
cursor = connection.cursor()
try:
    q = "INSERT INTO stock_temp_rawdata ( `Date`, `Close`, `Open`, `High`, `Low`, `Volume`) VALUES (%s, %s, %s, %s, %s, %s ); "
    cursor.executemany(q, args_list)
    # print(cursor._last_executed)
except pymysql.Error as e:
    print("Error %d: %s" % (e.args[0], e.args[1]))
    sys.exit()
    # sql = "insert into list_of_failed_stocks (equity_name,notes) VALUES (%s,'TD intake');"
    # cursor.execute(sql, equity)

# Clean the stock
equity = option_library.clean_stock('td2chad',equity)
print("Cleaned stock {}".format(equity));


description = "delete from stock_temp_rawdata where id = 1"
correct_date = "delete from stock_temp_rawdata where Date < %s"
correct_date2 = 'delete from stock_temp_rawdata where Date = "1969-12-31"'
correct_date3 = 'delete from stock_temp_rawdata where Date = "0000-00-00"'

load_up = "INSERT INTO " + equity + "_rawdata (Date, Open, High, Low, Close, volume) select Date, Open, High, Low, Close, Volume from stock_temp_rawdata order by id asc"

results33 = cursor.execute(description)
esults44 = cursor.execute(correct_date, clean_up_date)
results44 = cursor.execute(correct_date2)
results55 = cursor.execute(correct_date3)
results55 = cursor.execute(load_up)

# clean up
drop_temp = "truncate table stock_temp_rawdata;"
auto_ink = "alter table stock_temp_rawdata AUTO_INCREMENT = 0;"
results11m = cursor.execute(drop_temp)
results22m = cursor.execute(auto_ink)
# connection.close()

# Get the most recent day
nextDay = False
if nextDay == True:
    url = 'https://api.tdameritrade.com/v1/marketdata/quotes?apikey=KEY' +equity
    r = requests.get(url)
    payload = r.json()
    myFormat = "%Y-%m-%d"
    today = datetime.now()
    Date_daily_quotes = datetime.now().strftime('%Y-%m-%d')
    Date_daily_quotes= datetime.strptime(Date_daily_quotes, myFormat)
    Date_daily_quotes = "2019-02-05"
    print(Date_daily_quotes)
    for key, value in payload.items():
        args = [Date_daily_quotes, value['openPrice'], value['highPrice'], value['lowPrice'], value['regularMarketLastPrice'],value['totalVolume']]

    load_up_again = "INSERT INTO " + equity + "_rawdata ( `Date`, Open, High, Low, Close, volume) VALUES (STR_TO_DATE(%s, '%%Y-%%m-%%d'), %s, %s, %s, %s, %s ); "
    cursor.execute(load_up_again, args)
    print(cursor._last_executed)
    connection.close()

finish_equity = datetime.now()
print(finish_equity - start_equity)
