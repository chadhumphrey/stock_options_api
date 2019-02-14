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

connection = pymysql.connect(host='127.0.0.1', user='', password='', db='InteractiveB',
                             charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, connect_timeout=31536000)
cursor = connection.cursor()

#Clear table
q = "truncate stock_movers;"
cursor.execute(q)

index_list = ["$SPX.X","$DJI"]
for i in index_list:
    #up
    url = 'https://api.tdameritrade.com/v1/marketdata/'+i+'/movers?apikey=boomer72%40AMER.OAUTHAP&direction=up&change=percent'
    print(url)
    r = requests.get(url)
    payload = r.json()
    print(payload)
    for x in payload:
        print("key= {}  ".format(x['change']))
        print("key= {}  ".format(x['description']))
        print("key= {}  ".format(x['direction']))
        print("key= {}  ".format(x['last']))
        print("key= {}  ".format(x['symbol']))
        print("key= {}  ".format(x['totalVolume']))
        args = [x['change'], x['description'], x['direction'],
                x['last'], x['symbol'], x['totalVolume']]
        q = "INSERT INTO stock_movers ( `change`, `description`, `direction`, `last`, `symbol`, `totalVolume`) VALUES ( %s, %s, %s, %s, %s, %s); "
        cursor.execute(q, args)
        connection.commit()
        print(cursor._last_executed)

    #down
    url = 'https://api.tdameritrade.com/v1/marketdata/'+i+'/movers?apikey=boomer72%40AMER.OAUTHAP&direction=down&change=percent'
    print(url)
    r = requests.get(url)
    payload = r.json()
    print(payload)
    for x in payload:
        print("key= {}  ".format(x['change']))
        print("key= {}  ".format(x['description']))
        print("key= {}  ".format(x['direction']))
        print("key= {}  ".format(x['last']))
        print("key= {}  ".format(x['symbol']))
        print("key= {}  ".format(x['totalVolume']))
        args = [x['change'], x['description'], x['direction'],
                x['last'], x['symbol'], x['totalVolume']]
        q = "INSERT INTO stock_movers ( `change`, `description`, `direction`, `last`, `symbol`, `totalVolume`) VALUES ( %s, %s, %s, %s, %s, %s); "
        cursor.execute(q, args)
        connection.commit()
        print(cursor._last_executed)
