# /**
# **
# earning.py is based off the following respository https://github.com/wenboyu2/yahoo-earnings-calendar
# Simple script to scrap yahoo earnings dump them into a db
# **/

import pymysql
import pymysql.cursors
from datetime import datetime
import sys
from datetime import timedelta
from yahoo_earnings_calendar import YahooEarningsCalendar

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='benny',
                             db='stocks_bluesky',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()

#drop the table and start again
#dates can change, so you have to start from stratch
q = "truncate yahoo_earnings"
cursor.execute(q)

#set dates
now = datetime.now()
startDate = now.strftime('%b %d %Y %I:%M%p')
print(startDate)
endDate = now + timedelta(days=60)
endDate = endDate.strftime('%b %d %Y %I:%M%p')
print(endDate)
startDate = datetime.strptime(
    startDate, '%b %d %Y %I:%M%p')
endDate = datetime.strptime(
    endDate, '%b %d %Y %I:%M%p')

yec = YahooEarningsCalendar()
xx = yec.earnings_between(startDate, endDate)

print(xx)
sys.exit()
for x in xx:
    print("start--->{}".format(x['startdatetime']))
    print("ticker--->{}".format(x['ticker']))
    print("epsestimate--->{}".format(x['epsestimate']))
    print("epsactual--->{}".format(x['epsactual']))
    print("epssurprisepct--->{}".format(x['epssurprisepct']))

    try:
        q = "insert ignore into yahoo_earnings (earnings_date, ticker, epsestimate, epsactual, epssurprisepct) values (%s,%s,%s,%s,%s)"
        args = (x['startdatetime'], x['ticker'].lower(), x['epsestimate'],
                x['epsactual'], x['epssurprisepct'])
        cursor.execute(q, args)
        connection.commit()
        print(cursor._last_executed)
    except pymysql.InternalError as error:
        code, message = error.args
        print (">>>>>>>>>>>>>", code, message)

q = "truncate InteractiveB.upcoming_earnings"
cursor.execute(q)
connection.commit()
print(cursor._last_executed)

q = "INSERT INTO InteractiveB.upcoming_earnings (symbol, earnings_date ) SELECT DISTINCT(InteractiveB.IB_opt.symbol),stocks_bluesky.yahoo_earnings.earnings_date FROM 	InteractiveB.IB_opt,stocks_bluesky.yahoo_earnings WHERE InteractiveB.IB_opt.symbol = stocks_bluesky.yahoo_earnings.ticker ORDER BY stocks_bluesky.yahoo_earnings.earnings_date asc "
cursor.execute(q)
connection.commit()
print(cursor._last_executed)



# import datetime
# from yahoo_earnings_calendar import YahooEarningsCalendar
# # Returns the next earnings date of BOX in Unix timestamp
# yec = YahooEarningsCalendar()
# print yec.get_next_earnings_date('cmg')
