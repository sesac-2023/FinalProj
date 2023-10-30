# investing.com에서 데이터 크롤링
import requests
from bs4 import BeautifulSoup
from pykrx import stock
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
from sqlalchemy import create_engine
import sqlalchemy as db
import pymysql
import exchange_calendars as ecals


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}

# DB 로그인
db_connection_str = '주소 기입'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

today_percent = (datetime.datetime.today()-datetime.timedelta(1)).date()
today_str = today_percent.strftime("%Y-%m-%d")

    
code_name_sql = db.text(f"select code from stock_code_name where code_name = '{i[1]}'")
a = conn.execute(code_name_sql)
code = a.fetchone()[0]

response = requests.get(f"https://finance.naver.com/item/news_news.naver?code={code}&page=1&sm=title_entity_id.basic&clusterId=", headers=headers)

soup = BeautifulSoup(response.text, 'html.parser')