import time,requests,json,random
import pandas as pd
import datetime as dt

class StockCrawling:
    req = None
    db = None

    def __init__(self):
        self.req = requests.session()

    def __del__(self):
        self.req.close()

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

if __name__ == '__main__':
    # To ill stock IDs that you want.
    data = pd.read_csv("./StockID/化學類.csv")
    stock_ids = [ str(i) for i in data['代碼'].to_list() ]
    crawling = StockCrawling()
    try:
        while True:
            now = dt.datetime.now()
            #if now < STOP_TIME:
            data = crawling.show_realtime(*stock_ids)
            records = []
            for s_dict in data:
                try:
                    _t = dt.datetime.strptime(s_dict['t'], '%H:%M:%S').time()
                    _dt = dt.datetime.combine(dt.datetime.strptime(s_dict['d'], "%Y%m%d").date(), _t).strftime(
                    '%Y-%m-%d %H:%M:%S')
                    records.append([_dt, s_dict['c'], s_dict['n'],
                                float(s_dict['z']), float(s_dict['h']), float(s_dict['l']), float(s_dict['o']),
                                int(s_dict['v'])])
                except ValueError as _e:
                    print("ValueErr : " + str(_e))
            print(records)
            _sleep_time = random.randint(2, 4)
                # sleep
            time.sleep(_sleep_time)
            print("====== " + time.strftime("%H:%M:%S") + " ======")
    finally:
        del crawling



