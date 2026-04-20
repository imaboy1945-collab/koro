import os
import requests
import time
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pykrx import stock as krx

# GitHub Secrets에서 정보 가져오기
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
TOP_N     = 10
BLOG_URL  = "https://bestwellth.org"
MIN_SCORE = 40

def get_kospi200_tickers():
    try:
        now = datetime.now()
        for delta in range(10):
            d = (now - timedelta(days=delta)).strftime("%Y%m%d")
            try:
                tickers = krx.get_index_portfolio_depositary_receipt("1028", d)
                if len(tickers) > 0: return [f"{t}.KS" for t in tickers]
            except: continue
    except: pass
    return [f"{t}.KS" for t in ["005930", "000660", "373220", "207940", "005380"]]

_name_cache = {}
def get_stock_name(ticker):
    if ticker in _name_cache: return _name_cache[ticker]
    code = ticker.replace(".KS", "")
    try:
        name = krx.get_market_ticker_name(code)
        _name_cache[ticker] = name if name else code
        return _name_cache[ticker]
    except: return code

def send_telegram(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "disable_web_page_preview": False}
    requests.post(url, json=payload, timeout=10)

def calc_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    return 100 - (100 / (1 + (gain / loss.replace(0, np.nan))))

def calc_macd(series):
    ema_fast = series.ewm(span=12, adjust=False).mean()
    ema_slow = series.ewm(span=26, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    return macd_line, signal_line, macd_line - signal_line

def calc_bollinger(series):
    ma = series.rolling(20).mean()
    std = series.rolling(20).std()
    return ma + 2 * std, ma, ma - 2 * std

def analyze_ticker(ticker):
    try:
        df = yf.download(ticker, period="1y", progress=False, auto_adjust=True)
        if df is None or len(df) < 60: return None
        close, high, low, volume, opens = df["Close"].squeeze(), df["High"].squeeze(), df["Low"].squeeze(), df["Volume"].squeeze(), df["Open"].squeeze()
        
        signals, score = [], 0
        vol_ma20 = volume.rolling(20).mean()
        vol_ratio = (volume.iloc[-1] / vol_ma20.iloc[-1] - 1) * 100 if vol_ma20.iloc[-1] > 0 else 0
        body, candle_len = abs(close.iloc[-1] - opens.iloc[-1]), high.iloc[-1] - low.iloc[-1] + 1e-5

        if vol_ratio >= 300 and close.iloc[-1] > opens.iloc[-1] and (body / candle_len) > 0.5:
            score += 25; signals.append("V급증+몸통양봉(25)")
        rsi = calc_rsi(close)
        if rsi.iloc[-2] < 30 and 30 <= rsi.iloc[-1] < 45:
            score += 25; signals.append("RSI정밀바닥탈출(25)")
        _, _, bb_lower = calc_bollinger(close)
        if close.iloc[-2] < bb_lower.iloc[-2] and close.iloc[-1] > bb_lower.iloc[-1] and vol_ratio > 100:
            score += 25; signals.append("BB하단강력복귀(25)")
        ma5, ma20 = close.rolling(5).mean(), close.rolling(20).mean()
        if ma5.iloc[-1] > ma20.iloc[-1] and ma5.iloc[-2] <= ma20.iloc[-2] and (ma20.iloc[-1]/ma20.iloc[-5]-1) > -0.01:
            score += 20; signals.append("우상향5/20크로스(20)")
        _, _, hist = calc_macd(close)
        if close.iloc[-1] < ma20.iloc[-1] and hist.iloc[-2] < 0 and hist.iloc[-1] >= 0:
            score += 15; signals.append("MACD침체권크로스(15)")

        if score < MIN_SCORE: return None
        return {"ticker": ticker, "name": get_stock_name(ticker), "price": round(float(close.iloc[-1])),
                "change": round((close.iloc[-1]/close.iloc[-2]-1)*100, 2), "score": score, "signals": signals}
    except: return None

def run_scanner():
    tickers = get_kospi200_tickers()
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = [res for res in list(executor.map(analyze_ticker, tickers)) if res]
    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:TOP_N]
    
    if top:
        lines = [f"{i+1}. {d['name']} [{d['score']}점]\n   신호: {' / '.join(d['signals'])}\n   등락: {d['change']}% | 현재가: {d['price']:,}원" for i, d in enumerate(top)]
        msg = "📊 [인베스트웰스 코스피 200 스나이퍼 V3.1]\n━━━━━━━━━━━━━━\n\n" + "\n\n".join(lines) + f"\n\n상세 분석: {BLOG_URL}"
        send_telegram(msg)

if __name__ == "__main__":
    run_scanner()
