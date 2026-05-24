"""
종가매매법 텔레그램 알림봇
실행 시각: 매일 14:50 KST (GitHub Actions cron)
흐름: 데이터 수집 → 1차 기술적 필터 → LLM 분석 → 텔레그램 발송
"""

import os
import time
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pykrx import stock
from google import genai
from google.genai import types

# ─────────────────────────────────────────
# 환경변수
# ─────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
GEMINI_API_KEY   = os.environ.get("GEMINI_API_KEY", "")

# ─────────────────────────────────────────
# 상수
# ─────────────────────────────────────────
MIN_TOTAL_SCORE = 60          # 텔레그램 발송 최소 점수
MAX_CANDIDATES  = 5           # 최대 알림 종목 수
HIST_DAYS       = 90          # 기술 지표 계산용 과거 일수
VOL_RATIO_MIN   = 1.5         # 거래량 배율 하한
VOL_RATIO_MAX   = 10.0        # 거래량 배율 상한 (급등 세력 의심)
PRICE_CHG_MIN   = 2.0         # 당일 상승률 하한 (%)
PRICE_CHG_MAX   = 12.0        # 당일 상승률 상한 (%)
RSI_MIN         = 50.0        # RSI 하한 (기준선 위)
RSI_MAX         = 70.0        # RSI 상한 (과매수 전)
PREFILTER_TOP_N = 80          # 1차 빠른 필터 통과 후 상세 분석 대상 최대 수

# ─────────────────────────────────────────
# 지표 계산 함수
# ─────────────────────────────────────────

def calc_rsi(close: pd.Series, period: int = 14) -> float:
    """RSI 계산 — 마지막 값 반환 (avg_loss=0이면 RSI=100)"""
    delta    = close.diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    last_loss = float(avg_loss.iloc[-1])
    if last_loss == 0:
        return 100.0
    rs  = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


def calc_macd(close: pd.Series) -> float:
    """MACD = EMA(12) - EMA(26) — 마지막 값 반환"""
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    return float((ema12 - ema26).iloc[-1])


def calc_ma(close: pd.Series, period: int) -> float:
    """단순 이동평균 — 마지막 값 반환"""
    return float(close.rolling(period).mean().iloc[-1])

# ─────────────────────────────────────────
# 데이터 수집
# ─────────────────────────────────────────

def get_today_snapshot(market: str, today: str) -> pd.DataFrame:
    """
    pykrx: 오늘 전종목 OHLCV 스냅샷
    컬럼: 시가 고가 저가 종가 거래량 거래대금 등락률
    """
    try:
        df = stock.get_market_ohlcv_by_ticker(today, market=market)
        df.index.name = "ticker"
        df = df.reset_index()
        df["market"] = market
        return df
    except Exception as e:
        print(f"[경고] {market} 스냅샷 수집 실패: {e}")
        return pd.DataFrame()


def get_investor_flow(market: str, today: str) -> pd.DataFrame:
    """
    pykrx: 외인·기관 순매수 (오늘 기준)
    실패 시 빈 DataFrame 반환 — 수급 점수 0처리
    """
    try:
        df = stock.get_market_net_purchases_of_equities_by_ticker(
            today, today, market
        )
        return df
    except Exception as e:
        print(f"[경고] {market} 수급 데이터 수집 실패: {e}")
        return pd.DataFrame()


def prefilter(snapshot: pd.DataFrame) -> pd.DataFrame:
    """
    가격 변동·거래량 배율만으로 빠르게 후보 압축
    pykrx 등락률 컬럼명: '등락률'
    """
    # pykrx 컬럼 표준화
    rename = {
        "등락률": "price_chg",
        "거래량": "volume",
        "종가":   "close",
    }
    df = snapshot.rename(columns=rename)

    # 가격 변동 필터
    df = df[
        (df["price_chg"] >= PRICE_CHG_MIN) &
        (df["price_chg"] <= PRICE_CHG_MAX)
    ]

    # 거래량 상위 PREFILTER_TOP_N으로 압축
    df = df.nlargest(PREFILTER_TOP_N, "volume")
    return df.reset_index(drop=True)


def get_hist_metrics(ticker: str, today: str) -> dict | None:
    """
    종목별 과거 데이터로 RSI·MACD·MA 계산
    실패 시 None 반환
    """
    from_date = (datetime.strptime(today, "%Y%m%d") - timedelta(days=HIST_DAYS)).strftime("%Y%m%d")
    try:
        df = stock.get_market_ohlcv_by_date(from_date, today, ticker)
        if len(df) < 30:
            return None

        close  = df["종가"].astype(float)
        volume = df["거래량"].astype(float)

        vol_avg20  = float(volume.iloc[-21:-1].mean())
        vol_today  = float(volume.iloc[-1])
        vol_ratio  = vol_today / vol_avg20 if vol_avg20 > 0 else 0.0

        return {
            "rsi":       calc_rsi(close),
            "macd":      calc_macd(close),
            "ma5":       calc_ma(close, 5),
            "ma20":      calc_ma(close, 20),
            "vol_ratio": vol_ratio,
        }
    except Exception as e:
        print(f"[경고] {ticker} 과거 데이터 실패: {e}")
        return None

# ─────────────────────────────────────────
# 점수 계산
# ─────────────────────────────────────────

def calc_tech_score(row: dict) -> int:
    """기술적 점수 (70점 만점)"""
    score = 0

    # 거래량 (15점)
    if row["vol_ratio"] >= 1.5:
        score += 10
    if row["vol_ratio"] >= 3.0:
        score += 5

    # 가격 변동 (10점) — 3~8% 구간이 황금 구간
    chg = row["price_chg"]
    if 3.0 <= chg <= 8.0:
        score += 10
    elif (2.0 <= chg < 3.0) or (8.0 < chg <= 12.0):
        score += 5

    # 이동평균 (15점)
    if row["close"] > row["ma5"]:
        score += 10
    if row["close"] > row["ma20"]:
        score += 5

    # RSI (10점) — 50~65 황금 구간
    rsi = row["rsi"]
    if 50.0 <= rsi <= 65.0:
        score += 10
    elif 65.0 < rsi <= 70.0:
        score += 5

    # 수급 (20점)
    f_buy = row.get("foreign_net", 0) > 0
    i_buy = row.get("inst_net", 0) > 0
    if f_buy and i_buy:
        score += 20
    elif f_buy or i_buy:
        score += 10

    return score

# ─────────────────────────────────────────
# LLM 분석
# ─────────────────────────────────────────

def analyze_with_gemini(ticker: str, name: str, price_chg: float,
                         vol_ratio: float, tech_score: int) -> dict:
    """
    Gemini로 뉴스 분석 + 점수 산출
    반환: {news_grade, news_score, risk_deduction, reason, risk_note}
    """
    client = genai.Client(api_key=GEMINI_API_KEY)
    

    prompt = f"""
종목: {name} ({ticker})
당일 상승률: {price_chg:.1f}%
거래량 배율: {vol_ratio:.1f}배
기술적 점수: {tech_score}/70

다음 4가지 작업을 순서대로 수행하세요.

1. 오늘 이 종목의 주요 뉴스를 검색하세요.

2. 상승 원인을 아래 기준으로 분류하세요.
   A등급: 실적 서프라이즈, 수주 공시, 정부 정책 수혜 테마 (지속성 높음)
   B등급: 언론 노출, 업황 개선 기사 (지속성 보통)
   C등급: 원인 불명, 테마주 편승 의심 (지속성 낮음)

3. 아래 위험 신호가 있으면 차감 점수를 적용하세요.
   최대주주 매도 공시 확인 시: -15점
   유상증자 예정 확인 시: -10점
   조정 없이 연속 급등 3일 이상 확인 시: -10점

4. 뉴스 점수를 부여하세요.
   A등급: 25~30점 / B등급: 10~20점 / C등급: 0~5점

반드시 아래 JSON 형식으로만 응답하세요. 다른 텍스트나 마크다운 불필요.
{{
  "news_grade": "A 또는 B 또는 C",
  "news_score": 숫자,
  "risk_deduction": 숫자,
  "reason": "매수 근거 요약 (2줄 이내)",
  "risk_note": "위험 요소 내용 또는 없음"
}}
""".strip()

    fallback = {
        "news_grade": "C",
        "news_score": 0,
        "risk_deduction": 0,
        "reason": "LLM 분석 실패",
        "risk_note": "없음",
    }

    try:
        resp = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        text = resp.text.strip().replace("```json", "").replace("```", "").strip()
        result = json.loads(text)
        # 필드 존재 확인
        for key in fallback:
            result.setdefault(key, fallback[key])
        return result
    except Exception as e:
        print(f"[경고] {ticker} LLM 분석 실패: {e}")
        return fallback

# ─────────────────────────────────────────
# 텔레그램 발송
# ─────────────────────────────────────────

def send_telegram(message: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "HTML",
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"[오류] 텔레그램 발송 실패: {e}")


def build_message(candidates: list, timestamp: str) -> str:
    grade_emoji = {"A": "🟢", "B": "🟡", "C": "🔴"}

    lines = [f"📋 <b>오늘의 종가매매 후보</b> ({timestamp})\n"]

    for i, r in enumerate(candidates, 1):
        emoji = grade_emoji.get(r.get("news_grade", "C"), "⚪")
        lines.append(
            f"{i}. {emoji} <b>{r['name']}</b> ({r['ticker']}) "
            f"<b>{r['total_score']:.0f}점</b>"
        )
        lines.append(
            f"   현재가 {r['close']:,}원 | +{r['price_chg']:.1f}% | "
            f"거래량 {r['vol_ratio']:.1f}배"
        )
        lines.append(
            f"   RSI {r['rsi']:.1f} | MACD {'▲' if r['macd'] > 0 else '▼'} | "
            f"뉴스 {r['news_grade']}등급"
        )
        lines.append(f"   📌 {r['reason']}")
        if r.get("risk_note") and r["risk_note"] != "없음":
            lines.append(f"   ⚠️ {r['risk_note']}")
        lines.append("")

    lines.append("※ 본 알림은 참고용이며 투자 판단은 본인 책임입니다.")
    return "\n".join(lines)

# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────

def main():
    now   = datetime.now()
    today = now.strftime("%Y%m%d")
    ts    = now.strftime("%m/%d %H:%M")
    print(f"[{ts}] 종가매매 알림봇 시작 — 대상일: {today}")

    # ── 1. 오늘 스냅샷 수집 ──────────────────
    snapshots = []
    for market in ["KOSPI", "KOSDAQ"]:
        df = get_today_snapshot(market, today)
        if not df.empty:
            snapshots.append(df)

    if not snapshots:
        send_telegram("⚠️ 종가매매봇: 시장 데이터 수집 실패")
        return

    snapshot = pd.concat(snapshots, ignore_index=True)
    print(f"전체 종목 수: {len(snapshot)}")

    # ── 2. 빠른 1차 필터 (가격·거래량) ─────
    pre = prefilter(snapshot)
    print(f"1차 빠른 필터 통과: {len(pre)}종목")

    if pre.empty:
        send_telegram(f"📋 종가매매 후보 ({ts})\n조건 충족 종목 없음")
        return

    # ── 3. 수급 데이터 수집 ─────────────────
    investor_data = {}
    for market in ["KOSPI", "KOSDAQ"]:
        df_inv = get_investor_flow(market, today)
        if not df_inv.empty:
            for ticker in df_inv.index:
                investor_data[ticker] = {
                    "foreign_net": float(df_inv.loc[ticker].get("외국인", 0)),
                    "inst_net":    float(df_inv.loc[ticker].get("기관합계", 0)),
                }

    # ── 4. 종목별 기술 지표 계산 + 1차 필터 ─
    passed = []
    for _, row in pre.iterrows():
        ticker = row["ticker"]
        metrics = get_hist_metrics(ticker, today)
        if metrics is None:
            continue

        close = float(row["close"])
        vol_ratio = metrics["vol_ratio"]

        # 거래량 배율 필터
        if not (VOL_RATIO_MIN <= vol_ratio < VOL_RATIO_MAX):
            continue

        rsi  = metrics["rsi"]
        macd = metrics["macd"]
        ma5  = metrics["ma5"]

        # ── 필수 통과 조건 ──
        if not (RSI_MIN <= rsi <= RSI_MAX):   # RSI 50~70
            continue
        if macd <= 0:                          # MACD > 0 (기준선 위)
            continue
        if close <= ma5:                       # 현재가 > 5일선
            continue

        # 외인·기관 둘 다 순매도이면 제외
        inv = investor_data.get(ticker, {"foreign_net": 0, "inst_net": 0})
        if inv["foreign_net"] < 0 and inv["inst_net"] < 0:
            continue

        name = stock.get_market_ticker_name(ticker)

        passed.append({
            "ticker":      ticker,
            "name":        name,
            "market":      row.get("market", ""),
            "close":       close,
            "price_chg":   float(row["price_chg"]),
            **metrics,
            **inv,
        })

        time.sleep(0.15)  # pykrx 부하 방지

    print(f"기술적 필터 최종 통과: {len(passed)}종목")

    if not passed:
        send_telegram(f"📋 종가매매 후보 ({ts})\n기술적 조건 충족 종목 없음")
        return

    # ── 5. 기술적 점수 계산 ──────────────────
    for r in passed:
        r["tech_score"] = calc_tech_score(r)

    # 기술 점수 상위 15개만 LLM 분석 (API 비용·속도)
    passed.sort(key=lambda x: x["tech_score"], reverse=True)
    llm_targets = passed[:15]

    # ── 6. LLM 분석 ─────────────────────────
    results = []
    for r in llm_targets:
        llm = analyze_with_gemini(
            r["ticker"], r["name"],
            r["price_chg"], r["vol_ratio"], r["tech_score"]
        )
        total = r["tech_score"] + llm["news_score"] - llm["risk_deduction"]
        results.append({**r, **llm, "total_score": total})
        time.sleep(1.0)  # Gemini rate limit

    # ── 7. 최종 선별 ─────────────────────────
    final = [r for r in results if r["total_score"] >= MIN_TOTAL_SCORE]
    final.sort(key=lambda x: x["total_score"], reverse=True)
    final = final[:MAX_CANDIDATES]

    if not final:
        send_telegram(f"📋 종가매매 후보 ({ts})\n{MIN_TOTAL_SCORE}점 이상 종목 없음")
        return

    # ── 8. 텔레그램 발송 ─────────────────────
    msg = build_message(final, ts)
    send_telegram(msg)
    print(f"텔레그램 발송 완료 — {len(final)}종목")


if __name__ == "__main__":
    main()
