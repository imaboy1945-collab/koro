import os
import sys
import re
import time
import xml.etree.ElementTree as ET
import httpx
from datetime import datetime, timezone, timedelta

# ── 설정 (영어 원문 유지 필수) ──────────────────────────
GEMINI_API_KEY      = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID")

GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_URL   = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
TELEGRAM_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

KST = timezone(timedelta(hours=9))

def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    return re.sub(r"\s+", " ", text).strip()

def fetch_rss_news() -> str:
    articles = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    feeds = [
        ("매일경제", "https://www.mk.co.kr/rss/30100041"),
        ("한국경제", "https://www.hankyung.com/feed/stock"),
    ]
    with httpx.Client(timeout=30, follow_redirects=True, headers=headers) as client:
        for source, url in feeds:
            try:
                resp = client.get(url)
                if resp.status_code != 200: continue
                root = ET.fromstring(resp.content)
                for item in root.findall(".//item")[:5]:
                    title = strip_html(item.findtext("title"))
                    if title: articles.append(f"[{source}] {title}")
            except: continue
    return "\n".join(articles) if articles else "최신 뉴스를 가져오지 못했습니다."

def fetch_top5_volatile() -> str:
    today = datetime.now(KST).strftime("%Y%m%d")
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Referer": "http://data.krx.co.kr/contents/MDC/STAT/standard/MDCSTAT01501.jsp",
        "Origin": "http://data.krx.co.kr",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    results = {}
    with httpx.Client(timeout=30, headers=headers) as client:
        for mid, mname in [("STK", "코스피"), ("KSQ", "코스닥")]:
            try:
                client.get("http://data.krx.co.kr/contents/MDC/STAT/standard/MDCSTAT01501.jsp")
                payload = {"bld": "dbms/MDC/STAT/standard/MDCSTAT01501", "mktId": mid, "trdDd": today, "money": "1", "sortKey": "FLUC_RT", "pageSize": "5", "currentPage": "1"}
                r_up = client.post(url, data={**payload, "ascDesc": "desc"})
                r_down = client.post(url, data={**payload, "ascDesc": "asc"})
                data = r_up.json().get("output", []) + r_down.json().get("output", [])
                data.sort(key=lambda x: abs(float(x.get("FLUC_RT", 0))), reverse=True)
                results[mname] = data[:5]
            except: results[mname] = []
    
    lines = ["\n### 오늘의 변동성 Top5\n"]
    for mname, stocks in results.items():
        lines.append(f"**{mname}**")
        if not stocks: lines.append("- 데이터를 가져올 수 없습니다 (KRX 서버 제한)")
        for i, s in enumerate(stocks, 1):
            rate = float(s.get("FLUC_RT", 0))
            lines.append(f"- {i}. {s.get('ISU_ABBRV')} {'+' if rate>=0 else ''}{rate:.2f}% | {s.get('TDD_CLSPRC')}원")
    return "\n".join(lines)

def call_gemini(session: str, news_text: str) -> str:
    prompt = f"주식 애널리스트로서 오늘 {session}(pre:장전, post:장후) 브리핑을 작성하세요.\n뉴스:\n{news_text}\n\n형식: ## 제목, ### 소제목 사용. 800자 내외."
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    for i in range(3):
        try:
            with httpx.Client(timeout=60) as client:
                resp = client.post(GEMINI_URL, json=payload)
                if resp.status_code == 429:
                    time.sleep(30 * (i+1))
                    continue
                resp.raise_for_status()
                return resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            if i == 2: return f"Gemini 분석 실패 (원인: {str(e)[:50]})"
            time.sleep(10)
    return "Gemini 응답 지연"

def send_telegram(text: str) -> None:
    with httpx.Client(timeout=30) as client:
        client.post(TELEGRAM_URL, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"})

def main():
    session = sys.argv[1] if len(sys.argv) > 1 else "pre"
    news = fetch_rss_news()
    analysis = call_gemini(session, news)
    if session == "post": analysis += "\n" + fetch_top5_volatile()
    send_telegram(analysis)

if __name__ == "__main__":
    main()
