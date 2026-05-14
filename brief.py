import os
import sys
import re
import time
import xml.etree.ElementTree as ET
import httpx
from datetime import datetime, timezone, timedelta

# ── 설정 ────────────────────────────────────────────────
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
    차단 확률이 낮은 구글 뉴스를 포함하여 뉴스 수집
    """
    feeds = [
        ("구글뉴스", "https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko"),
        ("연합뉴스TV", "https://www.yonhapnewstv.co.kr/category/news/economy/stock-bond/feed/"),
        ("뉴시스", "https://www.newsis.com/RSS/economy.xml"),
        ("뉴스핌", "https://www.newspim.com/rss/economy"),
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }
    
    articles = []
    with httpx.Client(timeout=20, follow_redirects=True, headers=headers) as client:
        for source, url in feeds:
            try:
                resp = client.get(url)
                if resp.status_code != 200:
                    continue
                
                root = ET.fromstring(resp.content)
                items = root.findall(".//item")
                count = 0
                for item in items[:6]:
                    title = strip_html(item.findtext("title") or "")
                    desc = strip_html(item.findtext("description") or "")[:200]
                    if title:
                        articles.append(f"[{source}] {title}\n{desc}")
                        count += 1
                print(f"[RSS] {source}: {count}건 수집")
            except Exception as e:
                print(f"[RSS] {source} 오류: {e}")
                
    if articles:
        return "\n\n".join(articles)
    
    print("[RSS] 전체 실패 → 날짜 기반 프롬프트로 대체")
    return "__RSS_FAILED__"

# ── 변동성 Top5 (FinanceDataReader 최적화) ─────────────────────
def fetch_top5_volatile() -> str:
    """
    FinanceDataReader의 StockListing 기능을 사용하여 속도와 안정성 확보
    """
    lines = ["\n\n### 오늘의 변동성 Top5\n"]
    try:
        import FinanceDataReader as fdr
        import pandas as pd
        
        # KRX 전체 종목 시세 가져오기 (이 방식이 개별 조회보다 훨씬 빠르고 안정적임)
        # StockListing('KRX')는 당일 등락률을 포함하는 경우가 많음
        df_krx = fdr.StockListing('KRX')
        
        if df_krx is None or df_krx.empty:
            return "\n\n### 오늘의 변동성 Top5\n- 데이터 조회 실패 (시장을 불러올 수 없음)\n"

        for market_code, mname in [("KOSPI", "코스피"), ("KOSDAQ", "코스닥")]:
            # 해당 시장 데이터만 필터링
            df_market = df_krx[df_krx['Market'] == market_code].copy()
            
            if df_market.empty:
                lines.append(f"**{mname}**\n- 해당 시장 데이터 없음\n")
                continue

            # 등락률(ChgRate) 기준 정렬 (절댓값 기준)
            # 변수명은 FDR 버전에 따라 'ChgRate' 또는 'Chg'일 수 있음
            rate_col = 'ChgRate' if 'ChgRate' in df_market.columns else 'Changes'
            
            # 거래량이 너무 적은 종목 제외 (1,000주 미만)
            if 'Volume' in df_market.columns:
                df_market = df_market[df_market['Volume'] > 1000]

            df_market['AbsRate'] = df_market[rate_col].abs()
            top5 = df_market.sort_values(by='AbsRate', ascending=False).head(5)

            lines.append(f"**{mname}**")
            for i, (_, row) in enumerate(top5.iterrows(), 1):
                name = row['Name']
                rate = row[rate_col]
                price = row['Close']
                vol = row.get('Volume', 0)
                sign = "+" if rate > 0 else ""
                lines.append(f"- {i}. {name} {sign}{rate:.2f}% | {int(price):,}원 | 거래량 {int(vol):,}")
            lines.append("")

    except Exception as e:
        print(f"[FDR] 오류: {e}")
        lines.append("- 변동성 데이터를 가져오는 중 오류가 발생했습니다.")
        
    return "\n".join(lines)

# ── Gemini 호출 ──────────────────────────────────────────
def call_gemini(session: str, news_text: str) -> str:
    api_key = get_env("GEMINI_API_KEY")
    gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
    
    now_kst = datetime.now(KST).strftime("%Y년 %m월 %d일 %H:%M")
    
    if news_text == "__RSS_FAILED__":
        news_section = f"※ 현재 뉴스 RSS 수집에 일시적인 장애가 있습니다. {now_kst} 기준 최신 시장 트렌드와 지표를 바탕으로 브리핑을 작성해주세요."
    else:
        news_section = f"=== 수집된 뉴스 ===\n{news_text}\n=================="

    prompt = f"""
당신은 국내 주식시장 전문 애널리스트입니다. 현재 시각: {now_kst} (KST)
오늘 {'장전' if session == 'pre' else '장후'} 브리핑을 작성해주세요.

작성 가이드:
- {'장전: 미 증시 요약 및 오늘 국장 전망' if session == 'pre' else '장후: 오늘 마감 시황 및 주요 특징주 분석'}
- 주요 뉴스 섹션을 상세히 분석
- 이모지 사용 금지, 전문적인 톤 유지
- 제목은 ##, 소제목은 ### 사용
- 800~1200자 내외로 상세하게 작성
{news_section}
"""

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 2048},
    }

    for attempt in range(3):
        try:
            with httpx.Client(timeout=60) as client:
                resp = client.post(gemini_url, json=payload)
                resp.raise_for_status()
                return resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            print(f"[Gemini] 시도 {attempt + 1} 실패: {e}")
            time.sleep(10)
    return "AI 브리핑 생성에 실패했습니다."

# ── 텔레그램 전송 ────────────────────────────────────────
def send_telegram(text: str) -> None:
    bot_token = get_env("TELEGRAM_BOT_TOKEN")
    chat_id = get_env("TELEGRAM_CHAT_ID")
    tg_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    now_kst = datetime.now(KST).strftime("%Y.%m.%d %H:%M")
    session_type = "장전" if datetime.now(KST).hour < 12 else "장후"
    header = f"[국내 증시 {session_type} 브리핑 | {now_kst} KST]\n{'─' * 30}\n\n"
    
    full_message = header + text
    
    # 텔레그램 메시지 길이 제한(4000자) 대응
    chunks = [full_message[i:i+4000] for i in range(0, len(full_message), 4000)]
    
    with httpx.Client(timeout=30) as client:
        for chunk in chunks:
            payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "Markdown"}
            resp = client.post(tg_url, json=payload)
            if not resp.is_success:
                # 마크다운 오류 시 일반 텍스트로 재시도
                payload.pop("parse_mode")
                client.post(tg_url, json=payload)

# ── 메인 ────────────────────────────────────────────────
def main():
    session = sys.argv[1] if len(sys.argv) > 1 else "pre"
    print(f"[시작] {session} 모드 실행 중...")
    
    news = fetch_rss_news()
    analysis = call_gemini(session, news)
    
    if session == "post":
        # 장후 브리핑일 경우 변동성 데이터 추가
        market_data = fetch_top5_volatile()
        analysis += market_data
        
    send_telegram(analysis)
    print("[완료] 브리핑이 전송되었습니다.")

if __name__ == "__main__":
    main()
