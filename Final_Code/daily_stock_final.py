# 뉴스 크롤링
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

lstm_sql = db.text(f"select * from stock_lstm where Date = '{today_str}'")
today_recommend = pd.read_sql(lstm_sql, conn)
predict_code = today_recommend[['today_predict', 'code_name']].drop_duplicates()
predict_code = predict_code.values.tolist()

# 추천 종목과, today_predict를 가져온다. 
# predict열에서 일치된 행을 가젼온다. 
today_recommend.set_index(['code_name', 'predict'], inplace = True, drop = True)
result = []
for i in predict_code:
    try:
        tr = today_recommend.loc[i[1], i[0]].tolist()  # 만약 예측한 값이 기존에 나온 패턴이 아니면 패스
    except:
        continue
    if tr[0] < 0.5:  # 예상 수익률이 0.5 이하면 추천하지 않는다. 
        continue
    
    code_name_sql = db.text(f"select code from stock_code_name where code_name = '{i[1]}'")
    a = conn.execute(code_name_sql)
    code = a.fetchone()[0]

    response = requests.get(f"https://finance.naver.com/item/news_news.naver?code={code}&page=1&sm=title_entity_id.basic&clusterId=", headers=headers)

    soup = BeautifulSoup(response.text, 'html.parser')



    result.append([i[1]] + tr) # 코드명, 예상수익률, 첫날 등락률, 둘째날 등락률, 셋째날 등락률 ==> 현자가를 기반으로 
 
    # 설명: 미 10년물 국채금리, 나스닥100, wti, 금, 달러인덱스, 테마 ==>이부분 크롤링의 구현하기 출처 investing.com
    # 추천 된 종목의 20일치 주가 가져오기
    # [상승종목*, 주가 날짜, 종가, 5MA, 20MA, 60MA, 120MA, 추천종목명*, 추천날짜*, 예측종가, 예측 수익률*, 뉴스제목*, 뉴스url*, 코사인 유사도*, 설명*]

    final_sql = db.text(f"select code, code_name, Date, close, MA_5, MA_20, MA_60, MA_120, theme from stocks where code_name = '{i[1]}' order by Date desc limit 20")
    final_stock = pd.read_sql(final_sql, conn)
    final_stock.sort_values('Date', ascending=True, inplace=True)
    
    code_origin_sql = db.text(f"select origin_code_name from stock_peak_sim_filter where code_name = '{i[1]}' and Date = '{today_percent}'")
    a = conn.execute(code_origin_sql)
    origin_code = a.fetchone()[0]

    final_stock['origin_code'] = origin_code
    final_stock['recommend_Date'] = today_percent
    final_stock['predict_close'] = np.nan
    final_stock['predict_profit'] = tr[0]
    
    final_sim_sql = db.text(f"select cos_sim from stock_peak_sim_filter where Date = '{today_percent}' and code_name ='{i[1]}'")
    final_sim = conn.execute(final_sim_sql)
    similarity = final_sim.fetchone()[0]
    final_stock['similarity'] = similarity
    final_stock['news_title'] = [i.get_text() for i in soup.select('tbody .title a')] + ['']*(20-len(soup.select('tbody .title a')))
    final_stock['news_url'] = ["finance.naver.com"+i['href'] for i in soup.select('tbody .title a')] +['']*(20-len(soup.select('tbody .title a')))

    final_macro = db.text(f"select * from stock_macro where Date = '{today_percent}'")
    final_macro = pd.read_sql(final_macro, conn)
    title_macro = final_macro.columns[1:]
    value_macro = final_macro.values.tolist()[0][1:]
    result_macro = [f'{i} : {j}' for i, j in list(zip(title_macro, value_macro))]
    result_macro = '\n'.join(result_macro)

    final_stock['explain'] = [result_macro] + ['']*19

    final_close_1 = final_stock['close'].tolist()[19]*(1+tr[2]/100)
    final_close_2 = final_close_1*(1+tr[3]/100)
    final_close_3 = final_close_2*(1+tr[4]/100)
    final_close = [round(final_close_1, 2), round(final_close_2, 2), round(final_close_3, 2)]
    
    # 개장일 확인
    XKRX = ecals.get_calendar("XKRX") # 한국 코드
    stock_date = today_percent + datetime.timedelta(1)
    stock_seven = pd.date_range(stock_date.strftime("%Y-%m-%d"), periods=7, freq="D")
    stock_three = []
    for date in stock_seven:
        date_str = date.strftime("%Y-%m-%d")
        if XKRX.is_session(date_str):
            stock_three.append(date) 

    stock_three = stock_three[:3]


    predict_df = [[code,i[1]] + [stock_three[index]] + [np.nan]*6+ [origin_code, today_percent] + [k, tr[0], similarity, '', '', ''] for index, k in enumerate(final_close)]
    final_predict = pd.DataFrame(predict_df, columns = ['code','code_name','Date','close','MA_5','MA_20','MA_60','MA_120','theme','origin_code','recommend_Date','predict_close','predict_profit','similarity','news_title', 'news_url', 'explain'])
    final_stock = pd.concat([final_stock, final_predict], ignore_index=True)
    final_stock.to_sql(name='stock_final', con=db_connection, if_exists='append', index=False)
