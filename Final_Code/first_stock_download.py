# 처음 전 종목 데이터 가져오기 ********처음 실행********
from sqlalchemy import create_engine
import sqlalchemy as db
import pymysql
import pandas as pd
from pykiwoom.kiwoom import *
import datetime
import time
from tqdm import tqdm

# DB 로그인
db_connection_str = '주소 기입'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()
sql = db.text("select code from stocks group by code")
code = pd.read_sql(sql, conn)
code_done = code['code'].tolist()
# last_update = code.sort_values('Date')['Date'].tolist()[-1]

# 키움 로그인
kiwoom = Kiwoom()
kiwoom.CommConnect()

# 테마 종목 리스트
group = kiwoom.GetThemeGroupList(1)
group = [[i, group[i]] for i in group]

theme = []
for i in group:
    tickers = kiwoom.GetThemeGroupCode(i[1])
    theme.append([i[0], tickers])

# 전종목 종목코드
kospi = kiwoom.GetCodeListByMarket('0')
leets = kiwoom.GetCodeListByMarket('6')
etf = kiwoom.GetCodeListByMarket('8')
kosdaq = kiwoom.GetCodeListByMarket('10')


total_code = list(set(kospi)-set(etf)-set(leets)-set(code_done))+list(set(kosdaq)-set(code_done))
stock_list = {}
for i in total_code:
    name = kiwoom.GetMasterCodeName(i)
    if "ETN" in name:
        continue
    stock_list[i] = name

# 문자열로 오늘 날짜 얻기
now = datetime.datetime.now()
# now = now - datetime.timedelta(3)
print(now)
today = now.strftime("%Y%m%d")

# 전 종목의 일봉 데이터
codes = list(stock_list.keys()) # 남아있는 종목이 900개 이상이면 900개까지 자른다. 처음 데이터를 수집한다면 한 9번정도 반복
if len(codes) > 310:
    codes = codes[:310]
for code in tqdm(codes):
    dfs = []
    df = kiwoom.block_request("opt10081",
                          종목코드=code,
                          기준일자=today,
                          수정주가구분=1,
                          output="주식일봉차트조회",
                          next=0)
    print(df.head())
    dfs.append(df)

    count = 0
    while kiwoom.tr_remained and count <= 1: # 1800개까지 가져온다. 
        df = kiwoom.block_request("opt10081",
                                종목코드=code,
                                기준일자=today,
                                수정주가구분=1,
                                output="주식일봉차트조회",
                                next=2)
        dfs.append(df)
        count += 1
        time.sleep(0.4)
    
    time.sleep(0.4)

    df = pd.concat(dfs)
    df['일자'] = pd.to_datetime(df['일자'], format="%Y%m%d")
    df = df.set_index('일자')
    df = df.sort_index()
    df['현재가'] = df['현재가'].apply(int)
    df['거래량'] = df['거래량'].apply(int)
    df['등락률'] = df['현재가'].pct_change(1)*100
    a = df['등락률'].to_list()
    a = ['상' if i>0 else '하' if i<0 else '보' for i in a ]  
    a_pattern = []
    for k in range(len(a)):
        if k < 5:
            a_pattern.append('0')
        else:    
            a_pattern.append(''.join(a[k-5:k+1]))
    df['변동률20MA'] = df['거래량'].rolling(window = 20).mean()
    df['MA_5'] = df['현재가'].rolling(window = 5).mean()
    df['MA_20'] = df['현재가'].rolling(window = 20).mean()
    df['MA_60'] = df['현재가'].rolling(window = 60).mean()
    df['MA_120'] = df['현재가'].rolling(window = 120).mean()
    df['pattern'] = a_pattern
    df = df.dropna()
    df['order_1'] =( df['MA_5']-df['MA_20']).apply(lambda x: 1 if x >=0 else -1)  # 정배열이면 +1 역배열이면 -1
    df['order_2'] =( df['MA_20']-df['MA_60']).apply(lambda x: 1 if x >=0 else -1)  # 정배열이면 +1 역배열이면 -1
    df['order_3'] =( df['MA_60']-df['MA_120']).apply(lambda x: 1 if x >=0 else -1)  # 정배열이면 +1 역배열이면 -1
    df['order_total'] = df['order_1'] + df['order_2'] + df['order_3']  # 최고 3 최저 -3 매수는 5일 평균이 2 이상일 때 한다.
    df['order_two'] =df['order_total'].apply(lambda x: 1 if x>=2 else 0)
    df['order_total_ok'] = df['order_two'].rolling(window=10).mean() # 2이상 10일 유지해야한다. 이 값이 1일 때만 매수 허용
    df = df.dropna()
    df['변동률']= (df['거래량']-df['변동률20MA'])/df['변동률20MA']
    df['code'] = code
    df['code_name'] = stock_list[code]

    theme_list = [i[1] for i in theme]
    theme_result = []
    for index, i in enumerate(theme_list):
        if code in i:
            theme_result.append(theme[index][0])
    df['theme'] = '//'.join(theme_result)
    df['Date'] = df.index
    df = df[['code', 'code_name','Date','현재가','등락률', 'MA_5', 'MA_20', 'MA_60', 'MA_120', 'pattern', 'order_total_ok', '변동률', 'theme']]
    df = df.round(2)
    df.reset_index(inplace=True, drop=True)
    df.columns = ['code', 'code_name','Date','close','percent', 'MA_5', 'MA_20', 'MA_60', 'MA_120', 'pattern', 'order_total_ok', 'fluc', 'theme']
    df.to_sql(name='stocks', con=db_connection, if_exists='append', index=False)