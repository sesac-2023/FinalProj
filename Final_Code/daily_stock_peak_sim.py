# 코사인 유사도 종목 선정 후 바로 실행 

from itertools import *
import pymysql
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sqlalchemy as db
import pymysql
import pandas as pd
from pykiwoom.kiwoom import *
import datetime
import time
from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics.pairwise import euclidean_distances

# DB 로그인
db_connection_str = '주소 기입'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

sql = db.text("select code, code_name, group_concat(pat separator'') as pattern from (select code, code_name, case when percent > 0 then '상' when percent < 0 then '하' else '보' end as pat from stocks) a group by code")
code = pd.read_sql(sql, conn)
code
# 전체패턴리스트
dataset = ['상', '하', '보']
printList = list(product(dataset, repeat = 6))
printList = [''.join(i) for i in printList] 

stock_list = code['code_name'].tolist()
stock_code = code['code'].tolist()
total = []
for index, i in tqdm(enumerate(stock_list)):
    a = str(code['pattern'].iloc[index])
    result = [len(a.split(i))-1 for i in printList]
    total.append(result)

# 코사인, 거리 유사도 완료
cos_sim = pd.DataFrame(cosine_similarity(total, total), index = stock_list, columns = stock_list)
euc_sim = pd.DataFrame(euclidean_distances(total, total), index = stock_list, columns = stock_list)

# 종목별 피크 상위 2% 날짜 분석을 daily_stock_recommend_total을 사용
sql_recommend_total = db.text("select * from stocks_recommend_total")
recommend_total = pd.read_sql(sql_recommend_total, conn)
recommend_total_index = pd.read_sql(sql_recommend_total, conn)
recommend_total_index.set_index('Date', inplace=True)
# 당일 추천 종목을 가져온다. 
# today = datetime.datetime.today().strftime('%Y-%m-%d')
today = (datetime.datetime.today()-datetime.timedelta(1)).strftime('%Y-%m-%d')
sql_recommend_today = db.text(f"select * from stocks_recommend where Date = '{today}'")
recommend_today = pd.read_sql(sql_recommend_today, conn).iloc[0].tolist()[1:]

# 각 종목이 발견된 피크 날짜 딕셔너리 생성 
peak_dates_dic ={}
for code in tqdm(recommend_today):
    peak_dates = [] # 1~5일
    peaks = [] # 당일
    for i in range(len(recommend_total)):
        recommend_total_by_date = recommend_total.iloc[i].tolist()
        if code in recommend_total_by_date:
            peak = recommend_total_by_date[0]
            peaks.append(peak)
            start_date = (peak+ datetime.timedelta(1)).strftime("%Y-%m-%d")
            end_date = (peak+ datetime.timedelta(5)).strftime("%Y-%m-%d")
            peak_dates.append(pd.date_range(start_date, end_date, freq='D'))  # 각 종목의 피크 뜬 날 이후 5일간의 데이터
    dates = []
    for peak in peak_dates:
        for date in peak:
            dates.append(date) # 1차원으로 날짜를 푼다.
    dates = sorted(list(set(dates))) # 날짜 중복제거 및 오름차순 정렬, 이 날짜를 통해서 전체 종목들을 가져온다.
    
    total_codes = []
    for date in dates:
        try:
            total_codes.append(recommend_total_index.loc[date].tolist())
        except:
            continue  # 단순히 5일을 더한 날짜리스트는 휴장일이나 미래 날짜가 될 수도 있으므로 try_except구문에 넣는다. 
    total_codes = sum(total_codes,[])
    rank_codes = []
    for freq_code in list(set(total_codes)):
        rank_codes.append([freq_code, total_codes.count(freq_code)])
    
    rank_codes_df = pd.DataFrame(rank_codes, columns=['code_name', 'freq'])
    try:    
        rank_codes_df = rank_codes_df.dropna().sort_values('freq', ascending=False).iloc[:30] #조회수 상위 30개
    except:
        rank_codes_df = rank_codes_df.dropna().sort_values('freq', ascending=False) # 조회수 상위 30개 이하면 전체를 가져온다. 

    # 이제 종목을 선정 했으니 본 종목의 날짜 바로 다음날 수익률
    # 총 150개 중에 정배열 점수 1인 종목만 필터링 
    sql_order = db.text(f"select * from stocks where Date = '{today}' and order_total_ok >= 0.8")
    order_df = pd.read_sql(sql_order, conn)
    order_list = order_df['code_name'].tolist()
    rank_list = rank_codes_df['code_name'].tolist()
    filtered_list = list(set(rank_list)-set(set(rank_list)-set(order_list)))
    print(code, filtered_list, len(peaks))
    # 추천된 종목의 피크 다음날 부터 3일 중 첫번째 상에서 매도했을 때 수익률 
    if len(filtered_list) > 0:
        peak_dfs = []
        for peak in peaks:
            if peak > datetime.datetime.today() - datetime.timedelta(4): #오늘과 너무 가까운 날짜는 분석할 과거 데이터가 없기 때문에 제외한다. 
                continue
            peak_str = peak.strftime("%Y-%m-%d")
            for filtered in filtered_list:
                sql_peak = db.text(f"select Date, code_name, close, theme from stocks where Date >= '{peak}' and code_name = '{filtered}' limit 4") #피크 날짜부터 4일간 종가를 가져온다. 
                peak_df = pd.read_sql(sql_peak, conn)
                if len(list(set(peak_df['close'].tolist()))) == 1: # 거래 정지 제외
                    continue
                a = peak_df['close'].tolist()
                b = [a[1]-a[0], a[2]-a[0], a[3]-a[0]]
                c = [i for index, i in enumerate(b) if i>0 or index == 2] # 상승이 없으면 마지막 종가에 매도
                theme = peak_df['theme'].tolist()
                if len(theme)>1:
                    theme = theme[0]
                else:
                    theme = ''
                peak_list = [peak, filtered, round(c[0]/a[0]*100, 2), theme]         
                # 날짜, 종목명, 수익률, 테마

                peak_dfs.append(peak_list)
        peak_df_result = pd.DataFrame(peak_dfs, columns=['Date', 'code_name', 'profit', 'theme'])
        peak_df_total = pd.DataFrame()
        peak_df_total['profit'] = peak_df_result.groupby('code_name')['profit'].mean().round(2).tolist()
        peak_df_total['origin_code_name'] = code
        peak_df_total['code_name'] = filtered_list
        peak_df_total['cos_sim'] = cos_sim[code][filtered_list].round(2).tolist()
        peak_df_total['euc_sim'] = euc_sim[code][filtered_list].round(2).tolist()
        today_percent = (datetime.datetime.today()-datetime.timedelta(1)).date()
        today_str = today_percent.strftime("%Y-%m-%d")
        peak_df_total['Date'] = today_percent
        sql_today = db.text(f"select code_name, percent, theme from stocks where Date = '{today_str}'") 
        #오늘 등락률을 가져온다. 
        today_percent = pd.read_sql(sql_today, conn)
        today_percent.set_index('code_name', inplace = True)
        peak_df_total['percent'] = today_percent['percent'][filtered_list].round(2).tolist()
        theme = today_percent['theme'][filtered_list].tolist()
        theme = [str(index) if i=='' else i for index, i in enumerate(theme)]
        peak_df_total['theme'] = theme
        peak_df_total.drop(peak_df_total[(peak_df_total['percent'] == 0)].index, inplace=True)
        peak_df_total.dropna(inplace= True)
        peak_df_total.to_sql(name='stock_peak_sim_filter', con=db_connection, if_exists='append', index=False) 