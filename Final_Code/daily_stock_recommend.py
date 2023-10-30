# 당일 pf가 가장 높은 top 10 선정 ********매일 실행********
from sqlalchemy import create_engine
import sqlalchemy as db
import pymysql
import pandas as pd
from pykiwoom.kiwoom import *
import datetime
import time
from tqdm import tqdm

# 키움 로그인
db_connection_str = '주소 기입'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

sql = db.text("select Date from stocks_recommend")
code = pd.read_sql(sql, conn)
last_update = code.sort_values('Date', ascending =False)['Date'].tolist()[0]  # 마지막 업데이트 날짜 가져오기
last_update_sql = last_update.strftime("%Y-%m-%d")

sql_date = db.text("select Date from stocks order by Date desc limit 1") # 오늘 업데이트 된 stocks 테이블의 최신 날짜를 가져온다. 
last = pd.read_sql(sql_date, conn)['Date']
total_date = pd.date_range(last_update_sql, last.tolist()[0].strftime("%Y-%m-%d"), freq="1D").tolist()[1:]

peak_code = []
for date in tqdm(total_date): # 첫 200개만 가져오기, 하나가 업데이트 되면 하나 삭제해서 200개 유지시키기
    date_sql = date.strftime("%Y-%m-%d")
    sql = db.text(f"select code_name, percent/fluc as pf from stocks where Date = '{date_sql}' and fluc > 2 order by pf desc limit 10")
    peak_code.append([date]+pd.read_sql(sql, conn)['code_name'].tolist())

peak_code_df = pd.DataFrame(peak_code, columns=['Date', 'recommend_1', 'recommend_2', 'recommend_3', 'recommend_4', 'recommend_5', 'recommend_6', 'recommend_7', 'recommend_8', 'recommend_9', 'recommend_10'])
peak_code_df.to_sql(name='stocks_recommend', con=db_connection, if_exists='append', index=False)

# 200개 이상의 행은 삭제
sql = db.text("delete from stocks_recommend where Date < (select min(Date) from (select Date from stocks_recommend order by Date desc limit 200) a)") # 
conn.execute(sql)