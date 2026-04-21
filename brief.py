import os
import sys
import re
import time
import xml.etree.ElementTree as ET
import httpx
from datetime import datetime, timezone, timedelta

# ── 설정 ────────────────────────────────────────────────
# 환경변수는 함수 안에서 읽어야 None 타이밍 버그 방지
def get_env(key: str) -> str:
    val = os.environ.get(key, "")
    if not val:
        raise EnvironmentError(f"환경변수 {key} 가 설정되지 않았습니다.")
    return val

KST = timezone(timedelta(hours=9))


# ── RSS 뉴스 수집 ────────────────────────────────────────
def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    return re.sub(r"\s+", " ", text).strip()


def fetch_rss_news() -> str:
    """
    GitHub Actions에서 접근 가능한 RSS 피드만 사용.
    403 차단 없는 소스 위주로 구성.
    """
    feeds = [
        ("연합뉴스TV", "https://www.yonhapnewstv.co.kr/category/news/economy/stock-bond/feed/"),
        ("뉴시스",     "https://www.newsis.com/RSS/economy.xml"),
        ("뉴스핌",     "https://www.newspim.com/rss/economy"),
        ("파이낸셜",   "https://www.fnnews.com/rss/fn_realestate_stock.xml"),
    ]

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }

    articles = []
    with httpx.Client(timeout=20, follow_redirects=True, headers=headers) as client:
        for source, url in feeds:
            try:
                resp = client.get(url)
                if resp.status_code != 200:
                    print(f"[RSS] {source}: {resp.status_code} 건너뜀")
                    continue
                root  = ET.fromstring(resp.content)
                items = root.findall(".//item")
                count = 0
                for item in items[:6]:
                    title = strip_html(item.findtext("title") or "")
                    desc  = strip_html(item.findtext("description") or "")[:200]
                    if title:
                        articles.append(f"[{source}] {title}\n{desc}")
                        count += 1
                print(f"[RSS] {source}: {count}건 수집")
            except Exception as e:
                print(f"[RSS] {source} 오류: {e}")

    if articles:
        return "\n\n".join(articles)

    # 모든 RSS 실패 시 — Gemini에게 최신 정보 기반으로 작성 요청
    print("[RSS] 전체 실패 → 날짜 기반 프롬프트로 대체")
    return "__RSS_FAILED__"


# ── 변동성 Top5 (FinanceDataReader) ─────────────────────
def fetch_top5_volatile() -> str:
    """
    FinanceDataReader 라이브러리 사용.
    pykrx 대비 GitHub Actions 환경에서 안정적.
    """
    today = datetime.now(KST).strftime("%Y-%m-%d")
    lines = ["\n\n### 오늘의 변동성 Top5\n"]

    try:
        import FinanceDataReader as fdr
        import pandas as pd

        for market, mname in [("KOSPI", "코스피"), ("KOSDAQ", "코스닥")]:
            try:
                # 종목 리스트
                listing = fdr.StockListing(market)
                # 오늘 시세 (등락률 포함)
                df = fdr.DataReader(market, today, today)

                if df is None or df.empty:
                    lines.append(f"**{mname}**\n- 장 마감 데이터 없음 (휴장일 가능성)\n")
                    continue

                # 개별 종목 등락률 계산
                stocks_data = []
                for _, row in listing.iterrows():
                    code = row.get("Code") or row.get("Symbol", "")
                    name = row.get("Name", "")
                    if not code:
                        continue
                    try:
                        s = fdr.DataReader(code, today, today)
                        if s is None or s.empty:
                            continue
                        close   = float(s["Close"].iloc[-1])
                        change  = float(s["Change"].iloc[-1]) * 100  # 소수 → %
                        volume  = float(s["Volume"].iloc[-1])
                        # 거래량 0 필터
                        if volume < 1000:
                            continue
                        stocks_data.append({
                            "name":   name,
                            "rate":   change,
                            "price":  int(close),
                            "volume": int(volume),
                        })
                    except Exception:
                        continue

                if not stocks_data:
                    lines.append(f"**{mname}**\n- 종목 데이터 없음\n")
                    continue

                # 절댓값 기준 Top5
                stocks_data.sort(key=lambda x: abs(x["rate"]), reverse=True)
                top5 = stocks_data[:5]

                lines.append(f"**{mname}**")
                for i, s in enumerate(top5, 1):
                    sign = "+" if s["rate"] >= 0 else ""
                    lines.append(
                        f"- {i}. {s['name']}  "
                        f"{sign}{s['rate']:.2f}%  |  "
                        f"{s['price']:,}원  |  "
                        f"거래량 {s['volume']:,}"
                    )
                lines.append("")

            except Exception as e:
                print(f"[FDR] {mname} 오류: {e}")
                lines.append(f"**{mname}**\n- 데이터 조회 실패\n")

    except ImportError:
        # FinanceDataReader 미설치 — KRX 직접 호출 fallback
        print("[Top5] FinanceDataReader 미설치 → KRX 직접 호출")
        lines = ["\n\n### 오늘의 변동성 Top5\n"]
        lines += _fetch_krx_fallback()

    return "\n".join(lines)


def _fetch_krx_fallback() -> list:
    """KRX 직접 호출 (http 환경에서만 동작)"""
    today   = datetime.now(KST).strftime("%Y%m%d")
    url     = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Referer":    "http://data.krx.co.kr/contents/MDC/STAT/standard/MDCSTAT01501.jsp",
        "Origin":     "http://data.krx.co.kr",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    result_lines = []
    try:
        with httpx.Client(timeout=20, headers=headers) as client:
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
                combined.sort(key=lambda x: abs(float(x.get("FLUC_RT", 0))), reverse=True)

                result_lines.append(f"**{mname}**")
                for i, s in enumerate(combined[:5], 1):
                    rate  = float(s.get("FLUC_RT", 0))
                    sign  = "+" if rate >= 0 else ""
                    name  = s.get("ISU_ABBRV", "")
                    price = s.get("TDD_CLSPRC", "-")
                    result_lines.append(f"- {i}. {name}  {sign}{rate:.2f}%  |  {price}원")
                result_lines.append("")
    except Exception as e:
        print(f"[KRX fallback] 실패: {e}")
        result_lines.append("- 변동성 데이터를 가져올 수 없습니다.")
    return result_lines


# ── Gemini 호출 ──────────────────────────────────────────
def call_gemini(session: str, news_text: str) -> str:
    # ✅ 핵심 수정: URL은 함수 호출 시점에 키를 읽어 조합
    api_key    = get_env("GEMINI_API_KEY")
    gemini_url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.0-flash:generateContent?key={api_key}"
    )

    now_kst = datetime.now(KST).strftime("%Y년 %m월 %d일 %H:%M")

    if news_text == "__RSS_FAILED__":
        # RSS 실패 시 — 날짜 정보만 주고 Gemini 자체 지식으로 작성
        news_section = f"※ 뉴스 수집 실패. {now_kst} 기준 일반적인 시장 브리핑을 작성해주세요."
    else:
        news_section = f"=== 수집된 뉴스 ===\n{news_text}\n=================="

    if session == "pre":
        focus = """- 전일 미국 증시(다우, S&P500, 나스닥) 마감 현황 및 주요 이슈
- 전일 코스피·코스닥 종가 및 특이사항
- 오늘 국내 증시에 영향을 줄 핵심 변수 (지정학, 경제지표, 실적 등)
- 오늘 주목해야 할 섹터·종목
- 장전 전략 한 줄 요약"""
    else:
        focus = """- 오늘 코스피·코스닥 종가 및 등락률
- 오늘의 주요 수급 흐름 (외국인·기관·개인)
- 오늘 급등·급락 종목 및 이유
- 오늘 시장을 움직인 핵심 뉴스
- 내일 장 전망 및 주목 변수"""

    prompt = f"""당신은 국내 주식시장 전문 애널리스트입니다.
현재 시각: {now_kst} (KST)

오늘 {'장전' if session == 'pre' else '장후'} 브리핑을 작성해주세요.

{focus}

{news_section}

작성 규칙:
- 제목은 ##, 소제목은 ### 사용
- 글머리기호(-) 사용, 이모지 금지
- 뉴스에 없는 수치는 지어내지 말고 "확인 필요" 표기
- 800~1200자 이내
"""

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 2048},
    }

    for attempt in range(3):
        try:
            with httpx.Client(timeout=60) as client:
                resp = client.post(
                    gemini_url,
                    headers={"Content-Type": "application/json"},
                    json=payload,
                )
                if resp.status_code == 429:
                    wait = 30 * (attempt + 1)
                    print(f"[Gemini] Rate limit → {wait}초 대기")
                    time.sleep(wait)
                    continue
                if not resp.is_success:
                    print(f"[Gemini] HTTP {resp.status_code}: {resp.text[:200]}")
                    resp.raise_for_status()
                result = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
                print(f"[Gemini] 성공 ({len(result)}자)")
                return result
        except Exception as e:
            print(f"[Gemini] 시도 {attempt + 1} 실패: {e}")
            if attempt < 2:
                time.sleep(10)

    return "AI 분석을 가져오지 못했습니다. 잠시 후 다시 시도해주세요."


# ── 텔레그램 전송 ────────────────────────────────────────
def send_telegram(text: str) -> None:
    bot_token = get_env("TELEGRAM_BOT_TOKEN")
    chat_id   = get_env("TELEGRAM_CHAT_ID")
    tg_url    = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    now_kst      = datetime.now(KST).strftime("%Y.%m.%d %H:%M")
    session_type = "장전" if datetime.now(KST).hour < 12 else "장후"
    header       = f"[국내 증시 {session_type} 브리핑 | {now_kst} KST]\n{'─' * 30}\n\n"
    full_message = header + text

    chunks = [full_message[i:i + 4000] for i in range(0, len(full_message), 4000)]

    with httpx.Client(timeout=30) as client:
        for i, chunk in enumerate(chunks):
            payload = {
                "chat_id":                  chat_id,
                "text":                     chunk,
                "parse_mode":               "Markdown",
                "disable_web_page_preview": True,
            }
            resp = client.post(tg_url, json=payload)
            if not resp.is_success:
                # Markdown 파싱 오류 시 plain text 재시도
                payload["parse_mode"] = ""
                client.post(tg_url, json=payload)
            if i < len(chunks) - 1:
                time.sleep(1)

    print(f"[텔레그램] {len(chunks)}개 메시지 전송 완료")


# ── 메인 ────────────────────────────────────────────────
def main():
    session = sys.argv[1] if len(sys.argv) > 1 else "pre"
    assert session in ("pre", "post"), "인자는 'pre' 또는 'post' 여야 합니다."

    print(f"[시작] {session} 브리핑 | {datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')}")

    news     = fetch_rss_news()
    analysis = call_gemini(session, news)

    if session == "post":
        analysis += fetch_top5_volatile()

    send_telegram(analysis)
    print("[완료]")


if __name__ == "__main__":
    main()
