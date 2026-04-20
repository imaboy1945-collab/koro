import os
import sys
import re
import time
import xml.etree.ElementTree as ET
import httpx
from datetime import datetime, timezone, timedelta

# ── 설정 (GitHub Secrets에서 불러옴) ───────────────────────
GEMINI_API_KEY      = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID")

GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_URL   = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
)
TELEGRAM_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

KST = timezone(timedelta(hours=9))

# ── 뉴스 RSS 소스 ────────────────────────────────────────
RSS_FEEDS = [
    ("연합뉴스", "https://www.yonhapnewstv.co.kr/category/news/economy/stock-bond/feed/"),
    ("한국경제", "https://www.hankyung.com/feed/economy"),
    ("머니투데이", "https://news.mt.co.kr/RSS/newsRSS_stockNew.xml"),
    ("이데일리", "https://rss.edaily.co.kr/edaily/stock.xml"),
]

MAX_ARTICLES_PER_FEED = 5
MAX_TOTAL_CHARS       = 6000

# ── 뉴스 수집 함수 ────────────────────────────────────────
def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def fetch_rss_news() -> str:
    articles = []
    headers = {"User-Agent": "Mozilla/5.0"}

    with httpx.Client(timeout=20, follow_redirects=True) as client:
        for source, url in RSS_FEEDS:
            try:
                resp = client.get(url, headers=headers)
                resp.raise_for_status()
                root = ET.fromstring(resp.content)
                items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")

                count = 0
                for item in items:
                    if count >= MAX_ARTICLES_PER_FEED: break
                    title = item.findtext("title") or item.findtext("{http://www.w3.org/2005/Atom}title") or ""
                    desc  = item.findtext("description") or item.findtext("{http://www.w3.org/2005/Atom}summary") or ""
                    
                    title = strip_html(title)
                    desc  = strip_html(desc)[:300]
                    if title:
                        articles.append(f"[{source}] {title}\n{desc}")
                        count += 1
            except Exception as e:
                print(f"[RSS 오류] {source}: {e}")
                continue

    return "\n\n".join(articles) if articles else "수집된 뉴스 없음"

# ── KRX 변동성 수집 (장후용) ───────────────────────────────
def fetch_top5_volatile() -> str:
    today = datetime.now(KST).strftime("%Y%m%d")
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {
        "Referer": "http://data.krx.co.kr/",
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    results = {}

    for market_id, market_name in [("STK", "코스피"), ("KSQ", "코스닥")]:
        base = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
            "mktId": market_id,
            "trdDd": today,
            "money": "1",
            "sortKey": "FLUC_RT",
            "pageSize": "5",
            "currentPage": "1",
        }
        try:
            with httpx.Client(timeout=30) as client:
                r_up = client.post(url, headers=headers, data={**base, "ascDesc": "desc"})
                r_down = client.post(url, headers=headers, data={**base, "ascDesc": "asc"})
            combined = r_up.json().get("output", [])[:5] + r_down.json().get("output", [])[:5]
            combined.sort(key=lambda x: abs(float(x.get("FLUC_RT", 0))), reverse=True)
            results[market_name] = combined[:5]
        except:
            results[market_name] = []

    lines = ["\n### 오늘의 변동성 Top5\n"]
    for m_name, stocks in results.items():
        lines.append(f"**{m_name}**")
        for i, s in enumerate(stocks, 1):
            name = s.get("ISU_ABBRV", "알 수 없음")
            rate = float(s.get("FLUC_RT", 0))
            price = s.get("TDD_CLSPRC", "-")
            lines.append(f"- {i}. {name} {'+' if rate >= 0 else ''}{rate:.2f}% | {price}원")
    return "\n".join(lines)

# ── Gemini 분석 호출 ──────────────────────────────────────
def call_gemini(session: str, news_text: str) -> str:
    now_kst = datetime.now(KST).strftime("%Y년 %m월 %d일 %H:%M")
    focus = "장전 브리핑(미 증시 분석, 오늘 전략)" if session == "pre" else "장후 브리핑(오늘 수급, 특징주, 내일 전망)"
    
    prompt = f"""당신은 주식 애널리스트입니다. 시각: {now_kst}
{focus}에 맞춰 아래 뉴스를 요약/분석하세요.

뉴스 내용:
{news_text}

작성 규칙:
- 제목 ##, 소제목 ### 사용
- 불확실한 수치는 제외
- 800~1200자 이내, 이모지 사용 금지
"""

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 2048}
    }

    with httpx.Client(timeout=60) as client:
        resp = client.post(GEMINI_URL, json=payload)
        resp.raise_for_status()
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()

# ── 텔레그램 전송 ────────────────────────────────────────
def send_telegram(text: str) -> None:
    session_type = "장전" if datetime.now(KST).hour < 12 else "장후"
    header = f"[{session_type} 증시 브리핑 | {datetime.now(KST).strftime('%Y.%m.%d')}]\n\n"
    
    with httpx.Client(timeout=30) as client:
        client.post(TELEGRAM_URL, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": header + text,
            "parse_mode": "Markdown"
        })

# ── 메인 실행 ───────────────────────────────────────────
def main():
    session = sys.argv[1] if len(sys.argv) > 1 else "pre"
    print(f"[{session}] 브리핑 시작...")
    
    news = fetch_rss_news()
    analysis = call_gemini(session, news)
    
    if session == "post":
        analysis += "\n" + fetch_top5_volatile()
        
    send_telegram(analysis)
    print("전송 완료!")

if __name__ == "__main__":
    main()
