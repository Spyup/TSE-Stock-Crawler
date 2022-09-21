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

    def show_realtime(self, *stock_id, stock_type):
    # def show_realtime(self, *stock_id):
        twse_url = 'http://mis.twse.com.tw/stock/api/getStockInfo.jsp'
        timestamp = int(time.time() * 1000)
        # channels = '|'.join('tse_{}.tw'.format(target_tse) for target_tse in stock_id)
        for target in stock_type:
           if(target=='上市'):
               channels = '|'.join('tse_{}.tw'.format(target_tse) for target_tse in stock_id)
           elif(target=='上櫃'):
               channels = '|'.join('otc_{}.tw'.format(target_tse) for target_tse in stock_id)
        query_url = '{}?&ex_ch={}&json=1&delay=0&_={}'.format(twse_url, channels, timestamp)

        # headers = {'Accept-Language': 'zh-TW',
        #            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36',
        #            }

        headers = {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Host": "http://mis.twse.com.tw/stock/index.jsp",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "cross-site",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
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

        sql = "INSERT IGNORE INTO {}.test(catchtime, sdate, stime, sid, sname, price, thisamount, high, low, yesterday, open, amount) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(database)
        records = []
        stock_dict_data = stock_dict[0]
        stock_dict_time = stock_dict[1]
        _ct = dt.datetime.strptime(stock_dict_time,'%H:%M:%S').time().strftime('%H:%M:%S')

        for s_dict in stock_dict_data:
            try:
                _dt = dt.datetime.strptime(s_dict['d'], "%Y%m%d").date().strftime('%Y-%m-%d')
                _t = dt.datetime.strptime(s_dict['t'], '%H:%M:%S').time().strftime('%H:%M:%S')
                if(s_dict['z']!='-'):
                    s_dict['z'] = float(s_dict['z'])
                else:
                    s_dict['z'] = -1
                if(s_dict['tv']!='-'):
                    s_dict['tv'] = float(s_dict['tv'])
                else:
                    s_dict['tv'] = -1
                if(s_dict['h']!='-'):
                    s_dict['h'] = float(s_dict['h'])
                else:
                    s_dict['h'] = -1
                if(s_dict['l']!='-'):
                    s_dict['l'] = float(s_dict['l'])
                else:
                    s_dict['l'] = -1
                if(s_dict['o']!='-'):
                    s_dict['o'] = float(s_dict['o'])
                else:
                    s_dict['o'] = -1
                if(s_dict['v']!='-'):
                    s_dict['v'] = float(s_dict['v'])
                else:
                    s_dict['v'] = -1
                records.append([_ct, _dt, _t, s_dict['c'], s_dict['n'], s_dict['z'], s_dict['tv'],
                s_dict['h'], s_dict['l'], float(s_dict['y']), float(s_dict['o']), int(s_dict['v'])])              
            except ValueError as _e:
                print("ID : " + str(s_dict['c'])+"，ValueErr : " + str(_e))
            except KeyError as _e:
                print("ID : " + str(s_dict['c'])+"，KeyErr : " + str(_e))
        try:
            cursor.executemany(sql, records)
            self.db.commit()
            print(cursor.rowcount, "Record inserted successfully into python_users table")
        except pymysql.Error as _e:
            print("Failed inserting record into python_users table {}".format(_e))
        finally:
            cursor.close()

if __name__ == '__main__':
    # To ill stock IDs that you want.
    data = pd.read_csv("./stock_id_all.csv")
    stock_ids = [ str(i) for i in data['代碼'].to_list() ]
    stock_type = [ str(i) for i in data['櫃別'].to_list() ]

    stock_ids_1 = stock_ids[:100]
    stock_ids_2 = stock_ids[101:200]
    stock_ids_3 = stock_ids[201:300]
    stock_ids_4 = stock_ids[301:400]
    stock_ids_5 = stock_ids[501:600]
    stock_ids_6 = stock_ids[601:700]
    stock_ids_7 = stock_ids[701:800]
    stock_ids_8 = stock_ids[801:900]
    stock_ids_9 = stock_ids[901:1000]
    stock_ids_10 = stock_ids[1001:1100]
    stock_ids_11 = stock_ids[1101:1200]
    stock_ids_12 = stock_ids[1201:1300]
    stock_ids_13 = stock_ids[1301:1400]
    stock_ids_14 = stock_ids[1401:1500]
    stock_ids_15 = stock_ids[1501:1600]
    stock_ids_16 = stock_ids[1601:1700]
    stock_ids_17 = stock_ids[1701:]

    stock_type_1 = stock_type[:100]
    stock_type_2 = stock_type[101:200]
    stock_type_3 = stock_type[201:300]
    stock_type_4 = stock_type[301:400]
    stock_type_5 = stock_type[501:600]
    stock_type_6 = stock_type[601:700]
    stock_type_7 = stock_type[701:800]
    stock_type_8 = stock_type[801:900]
    stock_type_9 = stock_type[901:1000]
    stock_type_10 = stock_type[1001:1100]
    stock_type_11 = stock_type[1101:1200]
    stock_type_12 = stock_type[1201:1300]
    stock_type_13 = stock_type[1301:1400]
    stock_type_14 = stock_type[1401:1500]
    stock_type_15 = stock_type[1501:1600]
    stock_type_16 = stock_type[1601:1700]
    stock_type_17 = stock_type[1701:]
    STOP_TIME_HOUR = 13
    STOP_TIME_MINUTE = 32
    NOW_DATE = dt.datetime.now()
    STOP_TIME = dt.datetime(year=NOW_DATE.year,month=NOW_DATE.month,day=NOW_DATE.day,hour=STOP_TIME_HOUR, minute=STOP_TIME_MINUTE)
    crawling = StockCrawling()
    try:
        times = 1
        while True:
            now = dt.datetime.now()
            if now < STOP_TIME:
                # data = crawling.show_realtime(*stock_ids, stock_type=stock_type)
                if times == 1:
                    data = crawling.show_realtime(*stock_ids_1,stock_type=stock_type_1)
                elif times == 2:
                    data = crawling.show_realtime(*stock_ids_2,stock_type=stock_type_2)
                elif times == 3:
                    data = crawling.show_realtime(*stock_ids_3,stock_type=stock_type_3)
                elif times == 4:
                    data = crawling.show_realtime(*stock_ids_4,stock_type=stock_type_4)
                elif times == 5:
                    data = crawling.show_realtime(*stock_ids_5,stock_type=stock_type_5)
                elif times == 6:
                    data = crawling.show_realtime(*stock_ids_6,stock_type=stock_type_6)
                elif times == 7:
                    data = crawling.show_realtime(*stock_ids_7,stock_type=stock_type_7)
                elif times == 8:
                    data = crawling.show_realtime(*stock_ids_8,stock_type=stock_type_8)
                elif times == 9:
                    data = crawling.show_realtime(*stock_ids_9,stock_type=stock_type_9)
                elif times == 10:
                    data = crawling.show_realtime(*stock_ids_10,stock_type=stock_type_10)
                elif times == 11:
                    data = crawling.show_realtime(*stock_ids_11,stock_type=stock_type_11)
                elif times == 12:
                    data = crawling.show_realtime(*stock_ids_12,stock_type=stock_type_12)
                elif times == 13:
                    data = crawling.show_realtime(*stock_ids_13,stock_type=stock_type_13)
                elif times == 14:
                    data = crawling.show_realtime(*stock_ids_14,stock_type=stock_type_14)
                elif times == 15:
                    data = crawling.show_realtime(*stock_ids_15,stock_type=stock_type_15)
                elif times == 16:
                    data = crawling.show_realtime(*stock_ids_16,stock_type=stock_type_16)
                elif times == 17:
                    data = crawling.show_realtime(*stock_ids_17,stock_type=stock_type_17)
                
                crawling.insert_sql(data)
                _sleep_time = random.randint(1, 5)
                # sleep
                time.sleep(_sleep_time)
                print("====== " + time.strftime("%H:%M:%S") + " ======")
                times += 1
                if times == 18:
                    times = 1
            else:
                break
    except pymysql.Error as e:
        print("Failed inserting record into python_users table {}".format(e))
    finally:
        del crawling
