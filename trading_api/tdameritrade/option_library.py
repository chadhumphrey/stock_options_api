
import pymysql.cursors
from datetime import datetime
import operator
import math
import sys


class DBase:

    db_config = ('localhost', 'benny', 'boomer', 'optionsTD')

    def __init__(self):
        self.conn = pymysql.connect(*self.db_config)
        self.cur = self.conn.cursor(pymysql.cursors.DictCursor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

class DBase2:

    db_config = ('', '', '', '')

    def __init__(self):
        self.conn = pymysql.connect(*self.db_config)
        self.cur = self.conn.cursor(pymysql.cursors.DictCursor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()


def build_table(table):
    # Connect to the database
    db = DBase()

    clear_table = "drop TABLE if EXISTS " + table + ";"
    make_table = "CREATE TABLE " + table + "  (`id` int(10) NOT NULL AUTO_INCREMENT, `strike` float(15, 2) NOT NULL,  `symbol` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL, `last` float(15, 2) NULL DEFAULT NULL, `bid` float(15, 2) NULL DEFAULT NULL, `ask` float(15, 2) NULL DEFAULT NULL, `change` float(15, 2) NULL DEFAULT 0.00, `change_p` float(15, 2) NULL DEFAULT NULL, `volume` int(10) NULL DEFAULT NULL, `open_interest` int(10) NULL DEFAULT NULL, `TD_IVOL` float(10, 2) NULL DEFAULT 0.00, `date_results` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL, `opt_type` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL, `equity` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL, `ex_date` date NULL,   `date_of_quote` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  `d1` float(10, 3) NULL DEFAULT 0.000, `d2` float(10, 3) NULL DEFAULT 0.000, `call_price_intrinsic` float(10, 2) NULL DEFAULT 0.00, `put_price_intrinsic` float(10, 2) NULL DEFAULT 0.00, `premium` float(10,2) NULL DEFAULT NULL, `std` float(15, 4) NULL DEFAULT NULL, `mid_price` float(15, 2) NULL DEFAULT NULL, `todays_score` int(11) NOT NULL, `ITM` bit(15) NOT NULL, `HV_20D` float(15, 4) NOT NULL, `div_yield` float(15, 4) NOT NULL, `current_price` float(15, 2) NOT NULL, `probability` float(15, 2) NOT NULL, `probability_ivol` float(15, 2) NOT NULL, `ivol` float(15, 2) NOT NULL, `delta` float(15, 4) NULL DEFAULT NULL, `deltaTD` float(15, 4) NULL DEFAULT NULL, `gammaTD` float(15, 4) NULL DEFAULT NULL, `thetaTD` float(15, 4) NULL DEFAULT NULL, `vegaTD` float(15, 4) NULL DEFAULT NULL, `rhoTD` float(15, 4) NULL DEFAULT NULL, `days_to_expiration` int(11) NULL DEFAULT NULL,`time_value` float(15, 2) NOT NULL, `theoretical_volatility` float(15, 2) NOT NULL, `mark` float(15, 2) NOT NULL,`ask_size` float(15, 2) NOT NULL,`bid_size` float(15, 2) NOT NULL, `IBsymbol` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,PRIMARY KEY (`id`) USING BTREE) ENGINE = MyISAM AUTO_INCREMENT = 0 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;"
    db.cur.execute(clear_table)
    print(db.cur._last_executed)
    db.cur.execute(make_table)
    print(db.cur._last_executed)

def build_table_AWS(table):
    # Connect to the database
    db = DBase2()


    clear_table = "drop TABLE if EXISTS " + table + ";"
    make_table = "CREATE TABLE " + table + "  (`id` int(10) NOT NULL AUTO_INCREMENT, `strike` float(15, 2) NOT NULL,  `symbol` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL, `last` float(15, 2) NULL DEFAULT NULL, `bid` float(15, 2) NULL DEFAULT NULL, `ask` float(15, 2) NULL DEFAULT NULL, `change` float(15, 2) NULL DEFAULT 0.00, `change_p` float(15, 2) NULL DEFAULT NULL, `volume` int(10) NULL DEFAULT NULL, `open_interest` int(10) NULL DEFAULT NULL, `TD_IVOL` float(10, 2) NULL DEFAULT 0.00, `date_results` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL, `opt_type` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL, `equity` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL, `ex_date` date NULL,   `date_of_quote` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  `d1` float(10, 3) NULL DEFAULT 0.000, `d2` float(10, 3) NULL DEFAULT 0.000, `call_price_intrinsic` float(10, 2) NULL DEFAULT 0.00, `put_price_intrinsic` float(10, 2) NULL DEFAULT 0.00, `premium` float(10,2) NULL DEFAULT NULL, `std` float(15, 4) NULL DEFAULT NULL, `mid_price` float(15, 2) NULL DEFAULT NULL, `todays_score` int(11) NOT NULL, `ITM` bit(15) NOT NULL, `HV_20D` float(15, 4) NOT NULL, `div_yield` float(15, 4) NOT NULL, `current_price` float(15, 2) NOT NULL, `probability` float(15, 2) NOT NULL, `probability_ivol` float(15, 2) NOT NULL, `ivol` float(15, 2) NOT NULL, `delta` float(15, 4) NULL DEFAULT NULL, `deltaTD` float(15, 4) NULL DEFAULT NULL, `gammaTD` float(15, 4) NULL DEFAULT NULL, `thetaTD` float(15, 4) NULL DEFAULT NULL, `vegaTD` float(15, 4) NULL DEFAULT NULL, `rhoTD` float(15, 4) NULL DEFAULT NULL, `days_to_expiration` int(11) NULL DEFAULT NULL,`time_value` float(15, 2) NOT NULL, `theoretical_volatility` float(15, 2) NOT NULL, `mark` float(15, 2) NOT NULL,`ask_size` float(15, 2) NOT NULL,`bid_size` float(15, 2) NOT NULL, `IBsymbol` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,PRIMARY KEY (`id`) USING BTREE) ENGINE = MyISAM AUTO_INCREMENT = 0 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;"
    db.cur.execute(clear_table)
    # print(db.cur._last_executed)
    db.cur.execute(make_table)

def build_IB_symobl(d,putCall,strikePrice,equity):
    IB_ex_date = d.strftime('%y%m%d')
    if putCall == "CALL":
        opt_type = "C"
    else:
        opt_type = "P"

    if strikePrice > 99:
        if (strikePrice).is_integer() == False:
            frac, whole = math.modf(strikePrice)
            s=str(frac) + str(whole)

            frac, whole = math.modf(strikePrice)
            whole=str(whole)
            frac=str(frac)
            text = 'some string... this part will be removed.'
            whole_head,whole_mid,whole_tail = whole.partition('.')
            frac_head,frac_mid,frac_tail = frac.partition('.')
            # print("this is strike without the frac--->{}".format(str(frac_tail)))
            # print("this is strike without the whole--->{}".format(str(whole_head)))
            s=str(whole_head) + str(frac_tail)
            # print("this is strike without the sum of string--->{}".format(s))
            # print("this is length  string--->{}".format(len(s)))
            if len(s) == 2:
                IB_strike="0000" + s + "00"
            elif len(s) == 3:
                IB_strike="000" + s + "00"
            elif len(s) == 4:
                IB_strike="00" + s + "00"
            elif len(s) == 5:
                print("amzon")
                sys.exit()
                IB_strike="00" + s + "000"
        else:
            IB_strike="00" + str(int(strikePrice)) + "000"
    else:
        if (strikePrice).is_integer() == False:
            frac, whole = math.modf(strikePrice)
            whole=str(whole)
            frac=str(frac)
            text = 'some string... this part will be removed.'
            whole_head,whole_mid,whole_tail = whole.partition('.')
            frac_head,frac_mid,frac_tail = frac.partition('.')
            # whole_head = whole.partition('.')
            # print("this is strike without the frac--->{}".format(str(frac_tail)))
            # print("this is strike without the whole--->{}".format(str(whole_head)))
            s=str(whole_head) + str(frac_tail)
            # print("this is strike without the sum of string--->{}".format(s))
            # print("this is length  string--->{}".format(len(s)))
            # sys.exit()
            if len(s) == 2:
                # print("2")
                IB_strike="0000" + s + "00"
            elif len(s) == 3:
                # print("3")
                IB_strike="000" + s + "00"
            elif len(s) == 4:
                IB_strike="00" + s + "00"
            elif len(s) == 5:
                print("amzon")
                sys.exit()
                IB_strike="00" + s + "000"
        else:
            IB_strike="000" + str(int(strikePrice)) + "00"
        # print("this is strike without the decimal--->{}".format(IB_strike))

    IBsymbol=equity.upper() + IB_ex_date + opt_type + IB_strike
    # print(IBsymbol)
    return IBsymbol

def load_contract_table(args_list):

    # Connect to the database
    connection = pymysql.connect(host='192.168.0.191',
                                 user='',
                                 password='',
                                 db='InteractiveB',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()

    q = "INSERT INTO " + table + "( `strike`, `symbol`, `last`, `bid`, `ask`, `change`, `change_p`, `volume`, `open_interest`, `TD_IVOL`,  `opt_type`, `equity`, `ex_date`, `call_price_intrinsic`, `put_price_intrinsic`,   `ITM`,   `deltaTD`, `gammaTD`, `thetaTD`, `vegaTD`, `rhoTD`, `days_to_expiration`,time_value,theoretical_volatility,mark, bid_size,ask_size) VALUES (   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,  %s, %s, %s,%s, %s,%s,%s, %s); "
    cursor.executemany(q, args_list)
    connection.commit()

def clean_stock(broker,stock):
    # Connect to the database
    db = DBase()

    switcher = {
        "chad": "select * from DEV_stocks.conversion_stock_symbols where originial_ticker = %s ",
        "yahoo": "select * from DEV_stocks.conversion_stock_symbols where chad_converted_ticker = %s ",
        "yahoo2tdOptions": "select * from DEV_stocks.conversion_stock_symbols where chad_converted_ticker = %s ",
        "yahoo2chad": "select * from DEV_stocks.conversion_stock_symbols where yahoo_ticker = %s",
        "td": "select * from DEV_stocks.conversion_stock_symbols where chad_converted_ticker = %s",
        "td2chad": "select * from DEV_stocks.conversion_stock_symbols where td_ticker = %s"
        }
    switcher2 = {
        "chad": "chad_converted_ticker",
        "yahoo": "yahoo_ticker",
        "yahoo2chad": "chad_converted_ticker",
        "td": "td_ticker",
        "td2chad": "chad_converted_ticker",
        "yahoo2tdOptions":"td_options"
        }

    q = switcher.get(broker, "Fail")
    a = switcher2.get(broker, "Fail")
    print(q)
    db.cur.execute(q, stock)
    result = db.cur.fetchall()

    if result is None or db.cur.rowcount == 0:
        print("nope")
        return stock;
    else:
        for row in result:
            print("Boom --->{}" .format(row[a]))
        return row[a]
