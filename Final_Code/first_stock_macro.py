# 거시 경제 데이터 가져오기 
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
from sqlalchemy import create_engine
import sqlalchemy as db


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}

# DB 로그인
db_connection_str = '주소 기입'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

bond = pd.read_csv(r"C:\Users\cyhgp\Downloads\미국 10년물 국채 금리  채권수익률 과거 데이터.csv")
bond = bond[['날짜', '종가', '변동 %']]
bond.set_index('날짜', inplace=True, drop=True)
bond.index = [datetime.datetime.strptime(i, '%Y- %m- %d') for i in bond.index]


nasdaq100 = pd.read_csv(r"C:\Users\cyhgp\Downloads\나스닥 100 과거 데이터.csv")
nasdaq100 = nasdaq100[['날짜', '종가', '변동 %']]
nasdaq100.set_index('날짜', inplace=True, drop=True)
nasdaq100.index = [datetime.datetime.strptime(i, '%Y- %m- %d') for i in nasdaq100.index]

gold = pd.read_csv(r"C:\Users\cyhgp\Downloads\금 선물 과거 데이터.csv")
gold = gold[['날짜', '종가', '변동 %']]
gold.set_index('날짜', inplace=True, drop=True)
gold.index = [datetime.datetime.strptime(i, '%Y- %m- %d') for i in gold.index]

dollor = pd.read_csv(r"C:\Users\cyhgp\Downloads\미국 달러 지수 선물 과거 데이터.csv")
dollor = dollor[['날짜', '종가', '변동 %']]
dollor.set_index('날짜', inplace=True, drop=True)
dollor.index = [datetime.datetime.strptime(i, '%Y년 %m월 %d일') for i in dollor.index]

WTI = pd.read_csv(r"C:\Users\cyhgp\Downloads\WTI유 선물 과거 데이터.csv")
WTI = WTI[['날짜', '종가', '변동 %']]
WTI.set_index('날짜', inplace=True, drop=True)
WTI.index = [datetime.datetime.strptime(i, '%Y- %m- %d') for i in WTI.index]

total_macro = pd.concat([bond, nasdaq100, gold, dollor, WTI], axis=1)
total_macro.reset_index(inplace=True)
total_macro.columns = ['Date','bond 10 year', 'bond rate', 'nasdaq100', 'nasdaq rate', 'gold', 'gold rate', 'dollor index', 'dollor rate', 'WTI', 'WTI rate']
total_macro
total_macro.to_sql(name='stock_macro', con=db_connection, if_exists='append', index=False)