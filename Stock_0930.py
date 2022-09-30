import time,requests,json,random
import pandas as pd
import datetime as dt
import pymysql

class StockCrawling:
    req = None
    db = None

    def __init__(self):
        self.req = requests.session()
        self.init_db()

    def __del__(self):
        self.req.close()
        self.close_db()

    #def show_realtime(self, *stock_id, stock_type):
    def show_realtime(self, *stock_id):
        twse_url = 'http://mis.twse.com.tw/stock/api/getStockInfo.jsp'
        timestamp = int(time.time() * 1000)
        channels = '|'.join('tse_{}.tw'.format(target_tse) for target_tse in stock_id)
        #for target in stock_type:
        #    if(target=='上市'):
        #        channels = '|'.join('tse_{}.tw'.format(target_tse) for target_tse in stock_id)
        #    elif(target=='上櫃'):
        #        channels = '|'.join('otc_{}.tw'.format(target_tse) for target_tse in stock_id)
        query_url = '{}?&ex_ch={}&json=1&delay=0&_={}'.format(twse_url, channels, timestamp)

        headers = {'Accept-Language': 'zh-TW',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36',
                   }

        self.req.get('http://mis.twse.com.tw/stock/index.jsp', headers=headers)
        response = self.req.get(query_url)
        if response.text.strip() == '':
            return None

        # d: 日期, h: 最高, l: 最低, c:代號, n: 名稱, t:時間, o: 開盤, v: 交易量, z: 成交價
        # load JSON from TSE
        #print(response.text)
        content = json.loads(response.text)
        # print (content)
        self.req.cookies.clear()
        return content['msgArray'], content['queryTime']['sysTime']
        
    def init_db(self):
        self.db = pymysql.connect(
            host="127.0.0.1",  # 主機名
            user="thu",  # 用戶名
            passwd="Thu@201905",  # 密碼
            db="stocktescrawler",  # 數據庫名稱
            charset="utf8")

    def close_db(self):
        if self.db:
            self.db.close()

    def insert_sql(self, stock_dict):
        if not self.db:
            print("DB Not Connect")
            return

        
        database = "stocktescrawler"

        ''' ### SQL Table tse_stock
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

        if stock_dict is None:
            return None

        sql = "INSERT IGNORE INTO {}.important_stock (catchtime, sdate, stime, sid, sname, price, thisamount, high, low, yesterday, open, amount) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(database)
        records = []
        stock_dict_data = stock_dict[0]
        stock_dict_time = stock_dict[1]
        print(stock_dict_time)
        print(type(stock_dict_time))
        _ct = dt.datetime.strptime(stock_dict_time,'%H:%M:%S').time().strftime('%H:%M:%S')

        for s_dict in stock_dict_data:
            try:
                _dt = dt.datetime.strptime(s_dict['d'], "%Y%m%d").date().strftime('%Y-%m-%d')
                _t = dt.datetime.strptime(s_dict['t'], '%H:%M:%S').time().strftime('%H:%M:%S')
                if(self.search_sql(database, _dt, _t, s_dict['c'])):
                    sql_price = "SELECT price FROM {db}.important_stock WHERE sdate=`{sdate}` AND sid={sid} ORDER BY stime DESC".format(db=database, sdate=_dt, sid=s_dict['c'])
                    cursor.execute(sql_price)
                    last_price = cursor.fetchall()
                    print(last_price[0])
                    if(s_dict['z']!='-'):
                        s_dict['z'] = float(s_dict['z'])
                    else:
                        s_dict['z'] = "NULL"

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
        cursor = self.db.cursor()
        try:
            cursor.executemany(sql, records)
            self.db.commit()
            print(cursor.rowcount, "Record inserted successfully into python_users table")
        except pymysql.Error as _e:
            print("Failed inserting record into python_users table {}".format(_e))
        finally:
            cursor.close()

    def search_sql(self,database,_dt,_t,stockID):
        search_cursor = self.db.cursor()
        sql_same_time = "SELECT * FROM {db}.important_stock WHERE `sdate`={sdate} AND `stime`=`{stime}` AND `sid`={sid}".format(db=database, sdate=_dt, stime=_t, sid=stockID)
        count = search_cursor.execute(sql_same_time)
        search_cursor.close()
        if(count!=0):
            return False

        return True

if __name__ == '__main__':
    # To ill stock IDs that you want.
    data = pd.read_csv("./NewListedStockID/urgent_0930.csv")
    
    stock_ids = [ str(i) for i in data['代碼'].to_list() ]
    stock_ids_1 = stock_ids[:100]
    stock_ids_2 = stock_ids[101:200]
    stock_ids_3 = stock_ids[201:300]
    stock_ids_4 = stock_ids[301:]
    STOP_TIME_HOUR = 13
    STOP_TIME_MINUTE = 40
    NOW_DATE = dt.datetime.now()
    STOP_TIME = dt.datetime(year=NOW_DATE.year,month=NOW_DATE.month,day=NOW_DATE.day,hour=STOP_TIME_HOUR, minute=STOP_TIME_MINUTE)
    crawling = StockCrawling()
    try:
        times = 1
        while True:
            now = dt.datetime.now()
            if now > STOP_TIME:
                if times == 1:
                    data = crawling.show_realtime(*stock_ids_1)
                elif times == 2:
                    data = crawling.show_realtime(*stock_ids_2)
                elif times == 3:
                    data = crawling.show_realtime(*stock_ids_3)
                elif times == 4:
                    data = crawling.show_realtime(*stock_ids_4)
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
    except pymysql.Error as e:
        print("Failed inserting record into python_users table {}".format(e))
    finally:
        del crawling
