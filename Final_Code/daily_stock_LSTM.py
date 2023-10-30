from sqlalchemy import create_engine
import sqlalchemy as db
import pymysql
import pandas as pd
from pykrx import stock
import datetime
from tqdm import tqdm
from collections import Counter
import datetime
import pandas as pd
import numpy as np
from string import punctuation
import tensorflow
from keras.callbacks import EarlyStopping
Tokenizer = tensorflow.keras.preprocessing.text.Tokenizer
pad_seqeunces = tensorflow.keras.preprocessing.sequence.pad_sequences
to_categorical = tensorflow.keras.utils.to_categorical
Sequential = tensorflow.keras.models.Sequential
Embedding = tensorflow.keras.layers.Embedding
LSTM = tensorflow.keras.layers.LSTM
Dense = tensorflow.keras.layers.Dense



# DB 로그인
db_connection_str = '주소 기입'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

# peak로 추천된 종목 가져오기
today_percent = (datetime.datetime.today()-datetime.timedelta(1)).date()
today_str = today_percent.strftime("%Y-%m-%d")
sql_peak = db.text(f"select * from stock_peak_sim_filter where profit > 0.5 and percent < 1 and cos_sim >0.8 and Date = '{today_str}'")
# 과거 이익률이 0.5이상이고, 당일 등락률이 1이하이며 cos_sim은  0.8이상이고 Date는 오늘
peak_recommend = pd.read_sql(sql_peak, conn)
filtered_list = peak_recommend['code_name'].tolist()
# code 데이터 가져오기
# 한 종목씩 LSTM 진행 
for filtered in tqdm(filtered_list):
    sql_pattern = db.text(f"select code, code_name, close, percent, Date, pattern from stocks where code_name = '{filtered}'")
    pattern_start = pd.read_sql(sql_pattern, conn)
    price_close = pattern_start.groupby('code_name')['close'].apply((list))
    price_percent = pattern_start.groupby('code_name')['percent'].apply((list))
    pattern_result = pattern_start.groupby('code_name')['pattern'].apply((list))
    pattern_list = pattern_result.tolist()[0]
    pattern_list_train = pattern_list[:-100] 
    pattern_list_test = pattern_list[-100:]

    stock_list = pattern_result.index.tolist()[0]
    pattern_list_10 = [pattern_list_train[i:i + 10] for i in range(0, len(pattern_list_train), 10)]

    tokenizer = Tokenizer()
    tokenizer.fit_on_texts(pattern_list_10)
    vocab_size = len(tokenizer.word_index) + 1  #729개의 패턴 + 1
    sentences = []
    for sentence in pattern_list_10:
        encoded = tokenizer.texts_to_sequences([sentence])[0]  # 여기서도 서로 다른 토크나이저를 사용
        for i in range(1, len(encoded)):
            sentences.append(encoded[:i+1])  # 한개씩 늘리는 작업

    max_len = 10
    pad = pad_seqeunces(sentences, maxlen=max_len, padding='pre')


    Xs = pad[:,:-1] # 마지막을 제외한 나머지 문장요소
    ys = pad[:,-1] # 정답 역할을 하는 마지막 요소
    ys = to_categorical(pad[:,-1], num_classes = vocab_size)


    embedding_dim = 10
    hidden_units = 128

    early_stopping = EarlyStopping(patience = 10, monitor = 'loss')
    with tensorflow.device('/device:GPU:0'):
        model = Sequential()
        model.add(Embedding(vocab_size, embedding_dim))
        model.add(LSTM(hidden_units))
        model.add(Dense(vocab_size, activation = 'softmax'))
        model.compile(loss='categorical_crossentropy', optimizer = 'adam', metrics=['accuracy'])
        model.fit(Xs, ys, epochs=200, verbose=2, callbacks = [early_stopping])


    predicts = []
    price = price_close.tolist()[0][-100:]
    percent = price_percent.tolist()[0][-100:]


    def sentence_generator(model, current_word, n):
        init_word = current_word
        sentence = ''

        for _ in range(n):
            encoded = tokenizer.texts_to_sequences([current_word])
            encoded = pad_seqeunces(encoded, maxlen=10, padding='pre')

            predicted = model.predict(encoded)
            predicted = np.argmax(predicted, axis=1)

            for word, index in tokenizer.word_index.items():
                if index == predicted:
                    break

            current_word=current_word+' '+word

            sentence=sentence+ ' ' + word

        sentence = init_word+sentence
        return sentence

    # 훈련된 모델로 test데이터를 맞춰본다.
    for index, i in enumerate(pattern_list_test[:-3]):
        predict = sentence_generator(model, i, 3)
        a = price[index:index+4] # 당일 포함 종가 4일치 가져오기 
        b = [a[1]-a[0], a[2]-a[0], a[3]-a[0]]
        c = [i for index, i in enumerate(b) if i>0 or index == 2] # 상승이 없으면 마지막 종가에 매도
        ud = round(c[0]/a[0]*100, 2) # 최종 매도가

        predicts.append([predict[-3:], ud, percent[index], percent[index+1], percent[index+2], percent[index+3]])

        
    predict_result = pd.DataFrame(predicts, columns = ['predict', 'percent', 'percent_0', 'percent_1', 'percent_2', 'percent_3' ])
    predict_result = predict_result.groupby('predict')[['percent', 'percent_0', 'percent_1', 'percent_2', 'percent_3']].mean()
    predict_result['today_predict'] = sentence_generator(model, pattern_list_test[-1], 3)[-3:] # 마지막 날 예측값 생성
    predict_result['Date'] = today_percent
    predict_result=predict_result.sort_values('percent',ascending=False)

    predict_result.reset_index(inplace = True)
    predict_result['code_name'] = stock_list
    predict_result = predict_result.round(2)

    predict_result.to_sql(name='stock_lstm', con=db_connection, if_exists='append', index=False)