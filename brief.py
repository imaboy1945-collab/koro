import os
import sys
import re
import time
import xml.etree.ElementTree as ET
import httpx
from datetime import datetime, timezone, timedelta

# ── 설정 ────────────────────────────────────────────────
GEMINI_API_KEY     = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID")

# ✅ 수정 1: 모델명 변경 (1.5-flash → 2.0-flash, v1beta 명시)
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_URL   = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
)
TELEGRAM_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

KST = timezone(timedelta(hours=9))


# ── RSS 뉴스 수집 ────────────────────────────────────────
def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    return re.sub(r"\s+", " ", text).strip()

def fetch_rss_news() -> str:
    articles = []
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    feeds = [
        ("매일경제", "https://www.mk.co.kr/rss/30100041"),
        ("한국경제", "https://www.hankyung.com/feed/stock"),
    ]
    with httpx.Client(timeout=30, follow_redirects=True, headers=headers) as client:
        for source, url in feeds:
            try:
                resp = client.get(url)
                if resp.status_code != 200:
                    continue
                root = ET.fromstring(resp.content)
                for item in root.findall(".//item")[:5]:
                    title = strip_html(item.findtext("title") or "")
                    desc  = strip_html(item.findtext("description") or "")[:200]
                    if title:
                        articles.append(f"[{source}] {title}\n{desc}")
            except Exception as e:
                print(f"[RSS 오류] {source}: {e}")
                continue

    return "\n\n".join(articles) if articles else "최신 뉴스를 가져오지 못했습니다."


# ── 변동성 Top5 (KRX) ────────────────────────────────────
def fetch_top5_volatile() -> str:
    today = datetime.now(KST).strftime("%Y%m%d")

    # ✅ 수정 2: KRX는 http만 지원 → 실패 시 빈 결과 처리 강화
    # GitHub Actions에서 http 차단 시 pykrx(PyPI)로 대체
    krx_data = _fetch_krx_direct(today)
    if not krx_data:
        krx_data = _fetch_krx_pykrx(today)

    lines = ["\n### 오늘의 변동성 Top5\n"]
    for mname, stocks in krx_data.items():
        lines.append(f"**{mname}**")
        if not stocks:
            lines.append("- 데이터를 가져올 수 없습니다.")
        else:
            for i, s in enumerate(stocks, 1):
                rate  = float(s.get("FLUC_RT", 0))
                price = s.get("TDD_CLSPRC", "-")
                name  = s.get("ISU_ABBRV", "알 수 없음")
                sign  = "+" if rate >= 0 else ""
                lines.append(f"- {i}. {name}  {sign}{rate:.2f}%  |  {price}원")
        lines.append("")

    return "\n".join(lines)


def _fetch_krx_direct(today: str) -> dict:
    """KRX 직접 호출 (http). GitHub Actions에서 막힐 수 있음."""
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://data.krx.co.kr/contents/MDC/STAT/standard/MDCSTAT01501.jsp",
        "Origin": "http://data.krx.co.kr",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    results = {}
    try:
        with httpx.Client(timeout=20, headers=headers) as client:
            # 세션 쿠키 먼저 획득
            client.get("http://data.krx.co.kr/contents/MDC/STAT/standard/MDCSTAT01501.jsp")
            for mid, mname in [("STK", "코스피"), ("KSQ", "코스닥")]:
                base = {
                    "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
                    "mktId": mid, "trdDd": today,
                    "money": "1", "sortKey": "FLUC_RT",
                    "pageSize": "5", "currentPage": "1",
                    "csvxls_isNo": "false",
                }
                r_up   = client.post(url, data={**base, "ascDesc": "desc"})
                r_down = client.post(url, data={**base, "ascDesc": "asc"})
                combined = (
                    r_up.json().get("output", [])[:5]
                    + r_down.json().get("output", [])[:5]
                )
                combined.sort(
                    key=lambda x: abs(float(x.get("FLUC_RT", 0))), reverse=True
                )
                results[mname] = combined[:5]
        print("[KRX] 직접 호출 성공")
        return results
    except Exception as e:
        print(f"[KRX] 직접 호출 실패: {e}")
        return {}


def _fetch_krx_pykrx(today: str) -> dict:
    """
    ✅ 수정 3: pykrx 라이브러리로 대체 (HTTPS 지원, GitHub Actions 호환)
    requirements.txt 또는 workflow의 pip install에 pykrx 추가 필요
    """
    try:
        from pykrx import stock as krx

        results = {}
        for market, mname in [("KOSPI", "코스피"), ("KOSDAQ", "코스닥")]:
            df = krx.get_market_price_change(today, today, market=market)
            if df is None or df.empty:
                results[mname] = []
                continue

            df = df[df["거래량"] > 0].copy()
            df["abs_rate"] = df["등락률"].abs()
            top5 = df.nlargest(5, "abs_rate").reset_index()

            stocks = []
            for _, row in top5.iterrows():
                stocks.append({
                    "ISU_ABBRV":  row.get("종목명", "-"),
                    "FLUC_RT":    str(row.get("등락률", 0)),
                    "TDD_CLSPRC": str(int(row.get("종가", 0))),
                })
            results[mname] = stocks

        print("[KRX] pykrx 호출 성공")
        return results

    except Exception as e:
        print(f"[KRX] pykrx 호출 실패: {e}")
        return {"코스피": [], "코스닥": []}


# ── Gemini 호출 ──────────────────────────────────────────
def call_gemini(session: str, news_text: str) -> str:
    now_kst = datetime.now(KST).strftime("%Y년 %m월 %d일 %H:%M")

    if session == "pre":
        focus = """
- 전일 미국 증시(다우, S&P500, 나스닥) 마감 현황 및 주요 이슈
- 전일 코스피·코스닥 종가 및 특이사항
- 오늘 국내 증시에 영향을 줄 핵심 변수
- 오늘 주목해야 할 섹터·종목
- 장전 전략 한 줄 요약"""
    else:
        focus = """
- 오늘 코스피·코스닥 종가 및 등락률
- 오늘의 주요 수급 흐름 (외국인·기관·개인)
- 오늘 급등·급락 종목 및 이유
- 오늘 시장을 움직인 핵심 뉴스
- 내일 장 전망 및 주목 변수"""

    prompt = f"""당신은 국내 주식시장 전문 애널리스트입니다.
현재 시각: {now_kst} (KST)

오늘 {'장전' if session == 'pre' else '장후'} 브리핑을 아래 뉴스 기반으로 작성해주세요.
{focus}

=== 수집된 뉴스 ===
{news_text}
==================

작성 규칙:
- 제목은 ##, 소제목은 ### 사용
- 글머리기호(-) 사용, 이모지 금지
- 뉴스에 없는 수치는 지어내지 않음
- 800~1200자 이내
"""

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 2048},
    }

    for attempt in range(3):
        try:
            with httpx.Client(timeout=60) as client:
                resp = client.post(GEMINI_URL, json=payload)
                if resp.status_code == 429:
                    wait = 30 * (attempt + 1)
                    print(f"[Gemini] Rate limit, {wait}초 대기...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            print(f"[Gemini] 시도 {attempt+1} 실패: {e}")
            if attempt < 2:
                time.sleep(10)

    return "Gemini 분석 실패 — 뉴스 수집은 정상이나 AI 응답을 받지 못했습니다."


# ── 텔레그램 전송 ────────────────────────────────────────
def send_telegram(text: str) -> None:
    now_kst      = datetime.now(KST).strftime("%Y.%m.%d %H:%M")
    session_type = "장전" if datetime.now(KST).hour < 12 else "장후"
    header       = f"[국내 증시 {session_type} 브리핑 | {now_kst} KST]\n{'─' * 30}\n\n"
    full_message = header + text

    chunks = [full_message[i:i + 4000] for i in range(0, len(full_message), 4000)]

    with httpx.Client(timeout=30) as client:
        for i, chunk in enumerate(chunks):
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }
            resp = client.post(TELEGRAM_URL, json=payload)
            if not resp.is_success:
                payload["parse_mode"] = ""
                client.post(TELEGRAM_URL, json=payload)
            if i < len(chunks) - 1:
                time.sleep(1)

    print(f"[텔레그램] 전송 완료 ({len(chunks)}개 메시지)")


# ── 메인 ────────────────────────────────────────────────
def main():
    session = sys.argv[1] if len(sys.argv) > 1 else "pre"
    assert session in ("pre", "post"), "인자는 'pre' 또는 'post' 여야 합니다."

    print(f"[시작] {session} 브리핑")

    news     = fetch_rss_news()
    analysis = call_gemini(session, news)

    if session == "post":
        analysis += "\n" + fetch_top5_volatile()

    send_telegram(analysis)
    print("[완료]")


if __name__ == "__main__":
    main()
