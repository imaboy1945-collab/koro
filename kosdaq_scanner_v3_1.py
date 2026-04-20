import os
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pykrx import stock as krx

# GitHub Secrets 환경변수
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
TOP_N     = 10
BLOG_URL  = "https://bestwellth.org"
MIN_SCORE = 45

def get_kosdaq200_tickers():
    try:
        now = datetime.now()
        for delta in range(10):
            d = (now - timedelta(days=delta)).strftime("%Y%m%d")
            try:
                tickers = krx.get_index_portfolio_depositary_receipt("2028", d)
                if len(tickers) > 0: return [f"{t}.KQ" for t in tickers]
            except: continue
    except: pass
    return [f"{t}.KQ" for t in ["247540", "086520", "196170", "253450", "293490"]]

def get_stock_name(ticker):
    code = ticker.replace(".KQ", "")
    try:
        name = krx.get_market_ticker_name(code)
        return name if name else code
    except: return code

def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "disable_web_page_preview": False}
    try: requests.post(url, json=payload, timeout=10)
    except: pass

def calc_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    return 100 - (100 / (1 + (gain / loss.replace(0, np.nan))))

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
        ma5, ma20 = close.rolling(5).mean(), close.rolling(20).mean()
        if ma5.iloc[-1] > ma20.iloc[-1] and ma5.iloc[-2] <= ma20.iloc[-2]:
            score += 20; signals.append("5/20골든크로스(20)")

        if score < MIN_SCORE: return None
        return {"name": get_stock_name(ticker), "price": round(float(close.iloc[-1])),
                "change": round((close.iloc[-1]/close.iloc[-2]-1)*100, 2), "score": score, "signals": signals}
    except: return None

if __name__ == "__main__":
    # ✅ 연결 확인용 메시지
    send_telegram("🚀 [인베스트웰스] 코스닥 200 스캐너가 정상 가동되었습니다.")
    
    tickers = get_kosdaq200_tickers()
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = [res for res in list(executor.map(analyze_ticker, tickers)) if res]
    
    if results:
        results.sort(key=lambda x: x["score"], reverse=True)
        top = results[:TOP_N]
        lines = [f"{i+1}. {d['name']} [{d['score']}점]\n   신호: {' / '.join(d['signals'])}\n   등락: {d['change']}% | {d['price']:,}원" for i, d in enumerate(top)]
        send_telegram("📊 코스닥 200 스나이퍼 리포트\n━━━━━━━━━━━━━━\n\n" + "\n\n".join(lines) + f"\n\n상세 분석: {BLOG_URL}")
    else:
        send_telegram("ℹ️ 현재 코스닥 200 중 스나이퍼 조건(50점)을 충족하는 종목이 없습니다.")
