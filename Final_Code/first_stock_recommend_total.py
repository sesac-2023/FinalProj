# 당일 pf가 가장 높은 top 10 선정 ********처음 실행********
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
sql_date = db.text("select Date from stocks group by Date")
total_date = pd.read_sql(sql_date, conn)['Date'].tolist()

peak_code = []
for date in tqdm(total_date): 
    date_sql = date.strftime("%Y-%m-%d")
    sql = db.text(f"select code_name, percent/fluc as pf from stocks where Date = '{date_sql}' and fluc > 2 order by pf desc limit 50")
    peak_code.append([date]+pd.read_sql(sql, conn)['code_name'].tolist())
    if len(peak_code) > 10:
        peak_code_df = pd.DataFrame(peak_code, columns=['Date', 'recommend_1', 'recommend_2', 'recommend_3', 'recommend_4', 'recommend_5', 'recommend_6', 'recommend_7', 'recommend_8', 'recommend_9', 'recommend_10', 'recommend_11', 'recommend_12', 'recommend_13', 'recommend_14', 'recommend_15', 'recommend_16', 'recommend_17', 'recommend_18', 'recommend_19', 'recommend_20', 'recommend_21', 'recommend_22', 'recommend_23', 'recommend_24', 'recommend_25', 'recommend_26', 'recommend_27', 'recommend_28', 'recommend_29', 'recommend_30', 'recommend_31', 'recommend_32', 'recommend_33', 'recommend_34', 'recommend_35', 'recommend_36', 'recommend_37', 'recommend_38', 'recommend_39', 'recommend_40', 'recommend_41', 'recommend_42', 'recommend_43', 'recommend_44', 'recommend_45', 'recommend_46', 'recommend_47', 'recommend_48', 'recommend_49', 'recommend_50'])
        peak_code_df.to_sql(name='stocks_recommend_total', con=db_connection, if_exists='append', index=False)
        peak_code = []

peak_code_df = pd.DataFrame(peak_code, columns=['Date', 'recommend_1', 'recommend_2', 'recommend_3', 'recommend_4', 'recommend_5', 'recommend_6', 'recommend_7', 'recommend_8', 'recommend_9', 'recommend_10', 'recommend_11', 'recommend_12', 'recommend_13', 'recommend_14', 'recommend_15', 'recommend_16', 'recommend_17', 'recommend_18', 'recommend_19', 'recommend_20', 'recommend_21', 'recommend_22', 'recommend_23', 'recommend_24', 'recommend_25', 'recommend_26', 'recommend_27', 'recommend_28', 'recommend_29', 'recommend_30', 'recommend_31', 'recommend_32', 'recommend_33', 'recommend_34', 'recommend_35', 'recommend_36', 'recommend_37', 'recommend_38', 'recommend_39', 'recommend_40', 'recommend_41', 'recommend_42', 'recommend_43', 'recommend_44', 'recommend_45', 'recommend_46', 'recommend_47', 'recommend_48', 'recommend_49', 'recommend_50'])
peak_code_df.to_sql(name='stocks_recommend_total', con=db_connection, if_exists='append', index=False)
        