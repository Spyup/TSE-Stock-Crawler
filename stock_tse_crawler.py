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
            db="mydb",  # 數據庫名稱
            charset="utf8")

    def close_db(self):
        if self.db:
            self.db.close()

    def insert_sql(self, stock_dict):
        if not self.db:
            print("DB Not Connect")
            return

        cursor = self.db.cursor()
        database = "mydb"
        # d: 日期, h: 最高, l:最低, c:代號, n: 名稱, t:時間, o: 開盤, v: 交易量, z: 成交價
        ''' ### SQL Table tse_stock
        `tse_stock`.`sdatetime`,
        `tse_stock`.`sid`,
        `tse_stock`.`sname`,
        `tse_stock`.`price`,
        `tse_stock`.`high`,
        `tse_stock`.`low`,
        `tse_stock`.`open`,
        `tse_stock`.`amount`
        '''

        if stock_dict is None:
            return None

        sql = "INSERT IGNORE INTO {}.tse_stock(sdatetime, sid, sname, price, high, low, open, amount) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)".format(
            database)
        records = []
        # print(stock_dict)
        for s_dict in stock_dict:
            # print(s_dict)
            try:
                _t = dt.datetime.strptime(s_dict['t'], '%H:%M:%S').time()
                _dt = dt.datetime.combine(dt.datetime.strptime(s_dict['d'], "%Y%m%d").date(), _t).strftime(
                    '%Y-%m-%d %H:%M:%S')
                records.append([_dt, s_dict['c'], s_dict['n'],
                                float(s_dict['z']), float(s_dict['h']), float(s_dict['l']), float(s_dict['o']),
                                int(s_dict['v'])])
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
    STOP_TIME_HOUR = 13
    STOP_TIME_MINUTE = 40
    STOP_TIME = dt.datetime.now(hour=STOP_TIME_HOUR, minute=STOP_TIME_MINUTE)
    crawling = StockCrawling()
    try:
        while True:
            now = dt.datetime.now()
            if now < STOP_TIME:
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
