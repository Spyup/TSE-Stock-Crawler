# -*- coding: utf8 -*-
'''
tse  上市
otc  上櫃
http://mis.tse.com.tw/stock/fibest.jsp?stock=2388
'''
import time
import requests
import json
import datetime as dt
import MySQLdb
import random


class StockCrawling:
    req = None
    db = None

    def __init__(self):
        self.req = requests.session()
        self.init_db()

    def __del__(self):
        self.req.close()
        self.close_db()

    def show_realtime(self, *stock_id):
        twse_url = 'http://mis.twse.com.tw/stock/api/getStockInfo.jsp'
        timestamp = int(time.time() * 1000)
        channels = '|'.join('tse_{}.tw'.format(target_tse) for target_tse in stock_id)
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
        content = json.loads(response.text)
        self.req.cookies.clear()
        return content['msgArray']

    def init_db(self):
        self.db = MySQLdb.connect(
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

        cursor = self.db.cursor()
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

        sql = "INSERT IGNORE INTO {}.tse_stock(sdate, stime, sid, sname, price, thisamount, high, low, yesterday, open, amount) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(database)
        records = []
        # print(stock_dict)
        for s_dict in stock_dict:
            # print(s_dict)
            try:
                #_dt: 日期；_t: 時間；c: 股票代碼；n: 股票簡稱；z: 當盤成交價；tv: 當盤成交量；
                # h: 最高價；l: 最低價；y: 昨收；o: 開盤價；v: 累積成交量

                _dt = dt.datetime.strptime(s_dict['d'], "%Y%m%d").date().strftime('%Y-%m-%d')
                _t = dt.datetime.strptime(s_dict['t'], '%H:%M:%S').time().strftime('%H:%M:%S')
                #_dt = dt.datetime.combine(dt.datetime.strptime(s_dict['d'], "%Y%m%d").date(), _t).strftime('%Y-%m-%d %H:%M:%S')
                '''
                records.append([_dt, _t, s_dict['c'], s_dict['n'],
                                float(s_dict['z']), float(s_dict['h']), float(s_dict['l']), float(s_dict['o']),
                                int(s_dict['v'])])
                '''
                if(s_dict['z']!='-'):
                    s_dict['z'] = float(s_dict['z'])
                else:
                    s_dict['z'] = float('NaN')
                if(s_dict['tv']!='-'):
                    s_dict['tv'] = float(s_dict['tv'])
                else:
                    s_dict['tv'] = float('NaN')
                if(s_dict['h']!='-'):
                    s_dict['h'] = float(s_dict['h'])
                else:
                    s_dict['h'] = float('NaN')
                if(s_dict['l']!='-'):
                    s_dict['l'] = float(s_dict['l'])
                else:
                    s_dict['l'] = float('NaN')
                if(s_dict['o']!='-'):
                    s_dict['o'] = float(s_dict['o'])
                else:
                    s_dict['o'] = float('NaN')
                if(s_dict['v']!='-'):
                    s_dict['v'] = float(s_dict['v'])
                else:
                    s_dict['v'] = float('NaN')

                records.append([_dt, _t, s_dict['c'], s_dict['n'], s_dict['z'], s_dict['tv'],
                s_dict['h'], s_dict['l'], float(s_dict['y']), float(s_dict['o']), int(s_dict['v'])])

                
            except ValueError as _e:
                print("ValueErr : " + str(_e))

        try:
            cursor.executemany(sql, records)
            self.db.commit()
            print(cursor.rowcount, "Record inserted successfully into python_users table")
        except MySQLdb.Error as _e:
            print("Failed inserting record into python_users table {}".format(_e))
        finally:
            cursor.close()


if __name__ == '__main__':
    # To ill stock IDs that you want.
    stock_ids = ['2388', '2380']
    #STOP_TIME_HOUR = 13
    #STOP_TIME_MINUTE = 40
    #STOP_TIME = dt.datetime.now(hour=STOP_TIME_HOUR, minute=STOP_TIME_MINUTE)
    STOP_TIME = 134000
    crawling = StockCrawling()
    try:
        while True:
            now = dt.datetime.now().strftime("%H%M%S")
            if int(now) < STOP_TIME:
                data = crawling.show_realtime(*stock_ids)
                crawling.insert_sql(data)
                _sleep_time = random.randint(2, 4)
                # sleep
                time.sleep(_sleep_time)
                print("====== " + time.strftime("%H:%M:%S") + " ======")
    except MySQLdb.Error as e:
        print("Failed inserting record into python_users table {}".format(e))
    finally:
        del crawling
