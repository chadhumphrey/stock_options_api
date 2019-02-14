from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs
import requests
import ssl
import sys

import option_library
import pymysql.cursors
from datetime import datetime
import time
import subprocess

# DB
connection = pymysql.connect(host='localhost', user='', password='',
                             db='optionsTD', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, connect_timeout=31536000)
cursor = connection.cursor()

# Declare
start = datetime.now()
count = str(200)
myFormat = "%Y-%m-%d"
today = datetime.now()
start_date = today.strftime(myFormat)
equity_list = []
q = "SELECT equity_name from DEV_stocks.list_of_equities_tracked where equity_name != 'spx'  "
cursor.execute(q)
result_set = cursor.fetchall()
for row in result_set:
    equity_list.append(row['equity_name'])
    print(row['equity_name'])
    subprocess.call(['python3', 'option_chain_quotes.py',row['equity_name'] ])
    # time.sleep(2)
connection.close()
finish= datetime.now()
print(finish - start)
