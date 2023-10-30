# bot.py
# 텔레그램 봇: stock_final의 추천 종목마다 맨처음 행 데이터 메시지로 보낸다. 

import logging
from telegram import __version__ as TG_VER
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
import schedule 
import time
import telegram
import asyncio
from sqlalchemy import create_engine
import sqlalchemy as db
from telegram.ext import Updater
from telegram.ext import CommandHandler
from pykiwoom.kiwoom import *

# from telegram_gpt import chatgpt

try:
    from telegram import __version_info__
except ImportError:
    __version_info__ = (0, 0, 0, 0, 0)  # type: ignore[assignment]

if __version_info__ < (20, 0, 0, "alpha", 1):
    raise RuntimeError(
        f"This example is not compatible with your current PTB version {TG_VER}. To view the "
        f"{TG_VER} version of this example, "
        f"visit https://docs.python-telegram-bot.org/en/v{TG_VER}/examples.html"
    )
from telegram import ForceReply, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# DB 로그인
db_connection_str = '주소 기입'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

# 키움 로그인
kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)

def signal_finder():


    # 날짜 설정
    today_tele = (datetime.datetime.now() - datetime.timedelta(3)).date()
    today_tele_str = today_tele.strftime("%Y-%m-%d")

    tele_sql = db.text(f"select * from stock_final where recommend_Date = '{today_tele_str}' group by code")
    tele_df = pd.read_sql(tele_sql, conn)

    tele_df = tele_df[['code_name', 'origin_code', 'recommend_Date', 'predict_profit', 'similarity', 'news_title', 'explain']]

    tele_list = tele_df.values.tolist()
    texts = []
    for i in tele_list:
        text = f"{i[2]} 추천 {i[1]} 기반으로 {i[0]}종목이 추천되었습니다. 예상수익률은 {i[3]}%입니다."
        texts.append(text)
    texts = '\n'.join(texts)

    return texts


def send_order(code):

    # 주식계좌
    accounts = kiwoom.GetLoginInfo("ACCNO")
    stock_account = accounts[0]

    code_sql = db.text(f"select code from stock_code_name where code_name = '{code}'")
    code_data = conn.execute(code_sql)
    code_number= code_data.fetchone()[0]

    # 삼성전자, 10주, 시장가주문 매수
    kiwoom.SendOrder("시장가매수", "0101", stock_account, 1, code_number, 10, 0, "03", "")

    return f"{stock_account}계좌에서 {code}가 매수가 완료 되었습니다."


# Define a few command handlers. These usually take the two arguments update and
# context.
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    await update.message.reply_html(
        rf"안녕하세요 {user.mention_html()}대표님!",
        reply_markup=ForceReply(selective=True),
    )


async def signal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""
    response_message = signal_finder()
    await update.message.reply_text(response_message)



# async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
#     """Echo the user message."""
#     response_message = update.message.text  # chatgpt(update.message.text)는 요금문제로 안되니까 에코봇으로
#     await update.message.reply_text(response_message)

async def order(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Echo the user message."""
    response_message = send_order(update.message.text)  # chatgpt(update.message.text)는 요금문제로 안되니까 에코봇으로
    await update.message.reply_text(response_message)

token = "token"

def main() -> None:
    """Start the bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(token).build()

    # on different commands - answer in Telegram
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("today", signal))

    # on non command i.e message - echo the message on Telegram
    # application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, order))

    # Run the bot until the user presses Ctrl-C
    application.run_polling()


if __name__ == "__main__":
    main()






