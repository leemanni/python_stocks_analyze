from calendar import calendar
from datetime import datetime
import json
from threading import Timer
import pymysql
import pandas as pd
import requests
from bs4 import BeautifulSoup


class DBUpdater:
    def __init__(self):
        '''생성자 : Maria DB 연결 및 종목 코드 딕셔너리 생성'''
        self.conn = pymysql.connect(host='localhost', port=3306, db='INVESTAR',
                                    user='root', passwd='1234', autocommit=True, charset='utf8')
        print('__init__ : DB 연결 시작')
        with self.conn.cursor() as curs:
            sql = '''
                CREATE TABLE IF NOT EXISTS company_info (
                code VARCHAR(20) ,
                company VARCHAR(40),
                last_update DATE,
                PRIMARY KEY (code))
            '''

            curs.execute(sql)

            sql = '''
            CREATE TABLE IF NOT EXISTS daily_price (
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (code, date))
            '''
            curs.execute(sql)

        self.conn.commit()
        print('__init__ : DB 연결 종료')

        self.codes = dict()
        self.update_company_info()

    def __del__(self):
        '''소멸자 : DB 연결 해제'''
        self.conn.close()

    def read_krx_code(self):
        '''KRX로부터 상장 법인 목록 파일으 읽어와서 데이터 프레임으로 전환'''

        print('read_krx_code : excel API 송신 시작')

        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'

        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드': 'code',
                                  '회사명': 'company'})
        krx.code = krx.code.map('{:06d}'.format)
        print('read_krx_code : excel API 송신 종료')

        return krx

    def update_company_info(self):
        '''종목코드를 company_info 테이블에서 업데이트 한 후 딕셔너리로 저장'''

        print('update_company_info : DB-> company_info 업데이트 시작')

        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)

        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]

        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')
            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info (code, company, last"\
                        f"_update) VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] #{idx+1:04d} REPLACE INTO company_info "
                          f"VALUES ({code}, {company}, {today})")
                self.conn.commit()
                print('')
        print('update_company_info : DB-> company_info 업데이트 종료')

    def read_naver(self, code, company, pages_to_fetch):
        '''네이버 금융에서 주식 시세를 읽어서 데이터 프레임으로 반환'''
        print('read_naver : 시작')
        try:
            # ▶ 1
            # 종목의 마지막 페이지 가져오기
            print('▷  종목의 마지막 페이지 가져오기')
            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            html = requests.get(
                url, headers={'User-agent': 'Mozilla/5.0'}).text
            bs = BeautifulSoup(html, 'lxml')
            pgrr = bs.find("td", class_="pgRR")
            if pgrr is None:
                return None
            vStr_temp = str(pgrr.a['href']).split('=')

            pages = min(int(vStr_temp[-1]), pages_to_fetch)
            df = pd.DataFrame()

            # ▶2
            # 페이지 별로 데이터 추출하기
            print('▷  페이지 별로 데이터 추출하기')
            for page in range(1, pages + 1):
                url = '{}&page={}'.format(url, page)
                req = requests.get(url, headers={'User-agent': 'Mozilla/5.0'})
                df = df.append(pd.read_html(req.text, header=0)[0])
                time_now = datetime.now().strftime('%Y-%m-%D %H:%M')
                print('[{}] - {} ({}) => {} / {} page'.format(time_now,
                      company, code, page, pages))

            df = df.rename(columns={'날짜': 'date',
                                    '종가': 'close',
                                    '전일비': 'diff',
                                    '시가': 'open',
                                    '고가': 'high',
                                    '저가': 'low',
                                    '거래량': 'volumn'
                                    })
            df['date'] = df['date'].replace('.', '-')
            df = df.dropna()
            df[['close', 'diff', 'open', 'high', 'low', 'volumn']]\
                = df[['close', 'diff', 'open', 'high', 'low', 'volumn']].astype(int)

            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volumn']]
        except Exception as e:
            print('Error : {}'.format(e))
        finally:
            print('read_naver : 종료')
            return df

    def replace_into_db(self, df, num, code, company):
        '''네이버 금융에서 읽어온 주식 시세를 DB에 REPLACE'''
        print('replace_into_db : 시작')

        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f'replace into daily_price values("{code}", "{r.date}", "{r.open}","{r.high}","{r.low}","{r.close}","{r.diff}", "{r.volumn}")'
                curs.execute(sql)
            self.conn.commit()
            print('[{}] - replace into daily_price -  {} ({}) => {}  tuple'.format(datetime.now().strftime('%Y-%m-%d %H:%M'),
                                                                                   company, code, len(df)))
        print('replace_into_db : 종료')

    def update_daily_price(self, pages_to_fetch):
        '''KRX 상장 법이느이 주식 시세를 네이버로 부터 읽어서 DB 에 업데이트'''
        print('update_daily_price : 시작')

        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])
        print('update_daily_price : 종료')

    def excute_daily(self):
        '''실행 즉시 및 매일 오후 다섯시에 daily_price 테이블 업데이트'''
        print('excute_daily : 시작')

        # 상장 법인 목록을 DB에 저장 => update_company_info() 메소드
        self.update_company_info()

        # json 파일에 패치 정보 기록
        print('▶  json 파일에 패치 정보 기록')
        try:
            with open('config.json', 'r') as in_file:
                print('▷ 패치 업데이트')
                config = json.load(in_file)
                page_to_fetch = config['page_to_fetch']
        except Exception as e:
            with open('config.json', 'w') as out_file:
                print('▷ 최초 업데이트')
                page_to_fetch = 100
                config = {'page_to_fetch': 1}
                json.dump(config, out_file)

        print('▶  json 파일에 패치 정보 기록 완료 !!')

        self.update_daily_price(page_to_fetch)

        time_now = datetime.now()

        lastday = calendar.monthrange(time_now, time_now.month)[1]

        if time_now.month == 12 and time_now.day == lastday:
            time_next = time_now.replace(
                year=time_now.year + 1, month=1, day=1, hour=17, minute=0, second=0)
        elif time_now.day == lastday:
            time_next = time_now.replace(
                month=time_now.month + 1, day=1, hour=17, minute=0, second=0)
        else:
            time_next = time_now.replace(
                day=time_now.day + 1, hour=17, minute=0, second=0)

        time_diff = time_next - time_now
        secs = time_diff.seconds

        timer = Timer(secs, self.excute_daily)

        print('Wating for next update >>  ({})'.format(
            time_next.strftime('%Y-%m-%d %H:%M')))
        timer.start()


if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.excute_daily()
