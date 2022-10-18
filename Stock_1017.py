from nis import cat
from pickle import NONE
import time,requests,json,random,logging
import pandas as pd
import datetime as dt
import pymysql

''' 
### SQL Table tse_stock
`tse_stock`.`sdate`,            # 日期
`tse_stock`.`stime`,            # 時間
`tse_stock`.`sid`,              # 股票代碼
`tse_stock`.`sname`,            # 股票簡稱
`tse_stock`.`price`,            # 當盤成交價
`tse_stock`.`thisamount`        # 當盤成交量
`tse_stock`.`high`,             # 最高價
`tse_stock`.`low`,              # 最低價
`tse_stock`.`yesterday`,        # 昨收
`tse_stock`.`open`,             # 開盤價
`tse_stock`.`amount`            # 累積成交量
'''

class StockCrawling:

    def __init__(self):
        self.req = requests.session()
        self.dbName = "stocktest"
        self.dbtable = ""
        self.STOPTIME = ""
        self.logger = None
        self.init_db()

    def __del__(self):
        self.req.close()
        self.close_db()

    # catch Stock Info
    def show_realtime(self, *stock_id):
        twse_url = 'http://mis.twse.com.tw/stock/api/getStockInfo.jsp'
        timestamp = int(time.time() * 1000)
        channels = '|'.join('tse_{}.tw'.format(target_tse) for target_tse in stock_id)
        query_url = '{}?&ex_ch={}&json=1&delay=0&_={}'.format(twse_url, channels, timestamp)

        headers = {'Accept-Language': 'zh-TW',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36',
                   }
        try:
            self.req.get('http://mis.twse.com.tw/stock/index.jsp', headers=headers)
            response = self.req.get(query_url)
            if response.text.strip() == '':
                return None

            # d: 日期, h: 最高, l: 最低, c:代號, n: 名稱, t:時間, o: 開盤, v: 交易量, z: 成交價
            # load JSON from TSE
            content = json.loads(response.text)
            self.req.cookies.clear()
            return content['msgArray'], content['queryTime']['sysTime']
        except Exception as e:
            print("Request Error: {}",format(e))
            self.logger.info("Request Error: {}",format(e))
            return None
        
    def init_db(self):
        self.db = pymysql.connect(
            host="127.0.0.1",  # 主機名
            user="thu",  # 用戶名
            passwd="Thu@201905",  # 密碼
            db=self.dbName,  # 數據庫名稱
            charset="utf8")

    def close_db(self):
        if self.db:
            self.db.close()

    def create_table(self):
        cursor = self.db.cursor()
        sql_create = "CREATE TABLE `{table}` (`ID` bigint(20) NOT NULL AUTO_INCREMENT, `catchtime` time NOT NULL, `sdate` date NOT NULL, `stime` time NOT NULL, `sid` varchar(10) NOT NULL, `sname` varchar(20) NOT NULL, `price` float DEFAULT NULL, `thisamount` float DEFAULT NULL, `high` float DEFAULT NULL, `low` float DEFAULT NULL, `yesterday` float DEFAULT NULL, `open` float DEFAULT NULL, `amount` float DEFAULT NULL, PRIMARY KEY (`ID`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;".format(table=self.dbtable)
        #sql_key = "ALTER TABLE `{table}` ADD ;".format(table=self.dbtable)
        #sql_AI = "ALTER TABLE `{table}` MODIFY `ID` bigint(20) NOT NULL ;".format(table=self.dbtable)
        #sql_check = "SET FOREIGN_KEY_CHECKS=1;"
        if(cursor.execute(sql_create)):
            self.db.commit()
            return True
        return False

    def insert_sql(self, stock_dict):
        if not self.db:
            print("DB Not Connect")
            return

        if stock_dict is None:
            return None

        sql = "INSERT IGNORE INTO `{table}` (catchtime, sdate, stime, sid, sname, price, thisamount, high, low, yesterday, open, amount) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table=self.dbtable)
        records = []
        stock_dict_data = stock_dict[0]
        stock_dict_time = stock_dict[1]
        _ct = dt.datetime.strptime(stock_dict_time,'%H:%M:%S').time().strftime('%H:%M:%S')
        cursor = self.db.cursor()
        for s_dict in stock_dict_data:
            try:
                # Format datetime type
                _dt = dt.datetime.strptime(s_dict['d'], "%Y%m%d").date().strftime('%Y-%m-%d')
                _t = dt.datetime.strptime(s_dict['t'], '%H:%M:%S').time().strftime('%H:%M:%S')
                # Check whether has same time's data
                if(self.search_sql(_t, s_dict['c'])):
                    sql_price = "SELECT price FROM `{table}` WHERE sid='{sid}' ORDER BY catchtime DESC".format(table=self.dbtable, sid=s_dict['c'])
                    count_price = cursor.execute(sql_price)
                    last_price = "NULL"
                    # Catch the stock last price, if the price is zero, then replace it
                    if(count_price>0):
                        last_price = cursor.fetchall()
                        last_price = float(last_price[0][0])
                        print(last_price)

                    if(s_dict['z']!='-'):
                        s_dict['z'] = float(s_dict['z'])
                    else:
                        s_dict['z'] = last_price

                    if(s_dict['tv']!='-'):
                        s_dict['tv'] = float(s_dict['tv'])
                    else:
                        s_dict['tv'] = "NULL"

                    if(s_dict['h']!='-'):
                        s_dict['h'] = float(s_dict['h'])
                    else:
                        s_dict['h'] = "NULL"

                    if(s_dict['l']!='-'):
                        s_dict['l'] = float(s_dict['l'])
                    else:
                        s_dict['l'] = "NULL"

                    if(s_dict['o']!='-'):
                        s_dict['o'] = float(s_dict['o'])
                    else:
                        s_dict['o'] = "NULL"

                    if(s_dict['v']!='-'):
                        s_dict['v'] = float(s_dict['v'])
                    else:
                        s_dict['v'] = "NULL"

                    records.append([_ct, _dt, _t, s_dict['c'], s_dict['n'], s_dict['z'], s_dict['tv'],
                    s_dict['h'], s_dict['l'], float(s_dict['y']), float(s_dict['o']), int(s_dict['v'])])
            except ValueError as _e:
                print("ValueErr : " + str(_e))
                self.logger.info("ValueError in insert_sql, Error:\n {} \n",format(_e))
            except KeyError as _e:
                print("ID : " + str(s_dict['c'])+"，KeyErr : " + str(_e))
                self.logger.info("KeyError in insert_sql, Error:\n {} \n",format(_e))
        
        try:
            cursor.executemany(sql, records)
            self.db.commit()
            print(cursor.rowcount, "Record inserted successfully into python_users table")
        except pymysql.Error as _e:
            print("Failed inserting record into python_users table {}".format(_e))
            self.logger.info("pysql error in insert_sql, Error:\n {} \n",format(_e))
        finally:
            cursor.close()

    def search_sql(self,_t,stockID):
        search_cursor = self.db.cursor()
        sql_same_time = "SELECT * FROM `{table}` WHERE stime='{stime}' AND `sid`='{sid}'".format(table=self.dbtable, stime=_t, sid=stockID)
        count = search_cursor.execute(sql_same_time)
        search_cursor.close()
        if(count!=0):
            return False

        return True

def mainFunction(crawling):
    # To ill stock IDs that you want.
    data = pd.read_csv("./NewListedStockID/urgent_0930.csv")
    
    stock_ids = [ str(i) for i in data['代碼'].to_list() ]
    stock_ids_1 = stock_ids[:100]
    stock_ids_2 = stock_ids[101:200]
    stock_ids_3 = stock_ids[201:300]
    stock_ids_4 = stock_ids[301:]
    times = 1
    while True:
        now = dt.datetime.now()
        if now < crawling.STOPTIME:
            if times == 1:
                current = stock_ids_1
                data = crawling.show_realtime(*stock_ids_1)
            elif times == 2:
                current = stock_ids_2
                data = crawling.show_realtime(*stock_ids_2)
            elif times == 3:
                current = stock_ids_3
                data = crawling.show_realtime(*stock_ids_3)
            elif times == 4:
                current = stock_ids_4
                data = crawling.show_realtime(*stock_ids_4)

            if data == None:
                crawling.logger.info("Try again!\n")
                data = crawling.show_realtime(*current)

            if data != None:
                crawling.insert_sql(data)

            _sleep_time = random.randint(2, 4)
            # sleep
            time.sleep(_sleep_time)
            print("====== " + time.strftime("%H:%M:%S") + " ======")
            times += 1
            if times == 5:
                times = 1
        else:
            break

if __name__ == '__main__':
    Logger = logging.getLogger("Logger")
    Logger.setLevel(logging.INFO)
    fh = logging.FileHandler(filename="/var/log/TSE_Python/Stock_1017.log",mode='a')
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)-9s - %(filename)-8s : %(lineno)s line - %(message)s",datefmt="%Y/%m/%d %H:%M:%S")
    fh.setFormatter(fmt)
    Logger.addHandler(fh)
    STOP_TIME_HOUR = 13
    STOP_TIME_MINUTE = 40
    NOW_DATE = dt.datetime.now()
    STOP_TIME = dt.datetime(year=NOW_DATE.year,month=NOW_DATE.month,day=NOW_DATE.day,hour=STOP_TIME_HOUR, minute=STOP_TIME_MINUTE)
    crawling = StockCrawling()
    crawling.dbtable = dt.date(year=NOW_DATE.year,month=NOW_DATE.month,day=NOW_DATE.day)
    crawling.STOPTIME = STOP_TIME
    crawling.logger = Logger
    crawling.create_table()
    try:
        mainFunction()
    except pymysql.Error as e:
        print("Failed inserting record into python_users table {}".format(e))
        crawling.logger.info("pymsql Error:\n {}\n",format(e))
        mainFunction()
    except Exception as e:
        print("Exception {}",format(e))
        crawling.logger.info("Exception {}",format(e))
        mainFunction()
    finally:
        del crawling
