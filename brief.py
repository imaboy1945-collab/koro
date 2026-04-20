import os
import sys
import re
import time
import xml.etree.ElementTree as ET
import httpx
from datetime import datetime, timezone, timedelta

# ── 설정 ───────────────────────────────────────────────
GEMINI_API_KEY      = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID")

GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_URL   = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
TELEGRAM_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

KST = timezone(timedelta(hours=9))

# ── 뉴스 RSS 소스 (2026년 기준 작동 확인 주소로 업데이트) ──
RSS_FEEDS = [
    ("연합뉴스TV", "https://www.yonhapnewstv.co.kr/category/news/economy/feed/"),
    ("매일경제", "https://www.mk.co.kr/rss/30100041"),
    ("한국경제", "https://www.hankyung.com/feed/stock"),
]

def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    return re.sub(r"\s+", " ", text).strip()

def fetch_rss_news() -> str:
    articles = []
    # 브라우저처럼 보이게 하기 위한 헤더
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    with httpx.Client(timeout=30, follow_redirects=True, headers=headers) as client:
        for source, url in RSS_FEEDS:
            try:
                resp = client.get(url)
                if resp.status_code != 200: continue
                
                root = ET.fromstring(resp.content)
                items = root.findall(".//item")
                
                count = 0
                for item in items:
                    if count >= 5: break
                    title = strip_html(item.findtext("title"))
                    desc  = strip_html(item.findtext("description"))[:200]
                    if title:
                        articles.append(f"[{source}] {title}\n{desc}")
                        count += 1
                print(f"[RSS] {source} 수집 성공")
            except Exception as e:
                print(f"[RSS 오류] {source}: {e}")
                continue

    return "\n\n".join(articles) if articles else "현재 수집된 최신 뉴스가 없습니다. 일반적인 시장 상황을 분석해 주세요."

def fetch_top5_volatile() -> str:
    today = datetime.now(KST).strftime("%Y%m%d")
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {"Referer": "http://data.krx.co.kr/", "User-Agent": "Mozilla/5.0"}
    
    results = {}
    for mid, mname in [("STK", "코스피"), ("KSQ", "코스닥")]:
        try:
            payload = {"bld": "dbms/MDC/STAT/standard/MDCSTAT01501", "mktId": mid, "trdDd": today, "money": "1", "sortKey": "FLUC_RT", "pageSize": "5", "currentPage": "1"}
            with httpx.Client(timeout=20) as client:
                r_up = client.post(url, headers=headers, data={**payload, "ascDesc": "desc"})
                r_down = client.post(url, headers=headers, data={**payload, "ascDesc": "asc"})
            
            combined = r_up.json().get("output", [])[:5] + r_down.json().get("output", [])[:5]
            combined.sort(key=lambda x: abs(float(x.get("FLUC_RT", 0))), reverse=True)
            results[mname] = combined[:5]
        except: results[mname] = []

    lines = ["\n### 오늘의 변동성 Top5\n"]
    for mname, stocks in results.items():
        lines.append(f"**{mname}**")
        if not stocks: lines.append("- 데이터를 가져올 수 없습니다.")
        for i, s in enumerate(stocks, 1):
            rate = float(s.get("FLUC_RT", 0))
            lines.append(f"- {i}. {s.get('ISU_ABBRV')} {'+' if rate>=0 else ''}{rate:.2f}% | {s.get('TDD_CLSPRC')}원")
    return "\n".join(lines)

def call_gemini(session: str, news_text: str) -> str:
    now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    prompt = f"당신은 주식 애널리스트입니다. 현재 시각: {now_kst}\n세션: {session}\n\n뉴스 데이터:\n{news_text}\n\n위 뉴스를 요약하고 시장 영향을 분석하여 텔레그램 브리핑 형식으로 작성하세요. 소제목은 ###을 사용하고 이모지는 쓰지 마세요."

    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.3}}

    # 429 에러 대비 재시도 로직
    for i in range(3):
        try:
            with httpx.Client(timeout=60) as client:
                resp = client.post(GEMINI_URL, json=payload)
                if resp.status_code == 429:
                    print(f"Gemini API 제한 발생(429). {10*(i+1)}초 후 재시도...")
                    time.sleep(10 * (i+1))
                    continue
                resp.raise_for_status()
                return resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            if i == 2: return f"Gemini 분석 실패: {e}"
            time.sleep(5)
    return "Gemini 분석에 실패했습니다."

def send_telegram(text: str) -> None:
    # 텔레그램 메시지 길이 제한(4096자) 대응
    if len(text) > 4000: text = text[:4000] + "..."
    
    try:
        with httpx.Client(timeout=30) as client:
            resp = client.post(TELEGRAM_URL, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"})
            resp.raise_for_status()
    except Exception as e:
        print(f"텔레그램 전송 실패: {e}")

def main():
    session = sys.argv[1] if len(sys.argv) > 1 else "pre"
    print(f"[{session}] 브리핑 프로세스 시작")
    
    news = fetch_rss_news()
    analysis = call_gemini(session, news)
    
    if session == "post":
        analysis += "\n" + fetch_top5_volatile()
        
    send_telegram(analysis)
    print("모든 작업 완료")

if __name__ == "__main__":
    main()
