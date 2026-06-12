"""
KOSDAQ 150 사전 발굴 스캐너
- 이미 터진 종목이 아닌, 올라가기 전 종목을 선별
- 6가지 사전 신호 점수화 → 4점 이상 텔레그램 발송
- 실행 시각: 15:45 KST (당일 데이터 게시 후)
"""

import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pykrx import stock

# ─────────────────────────────────────────
# 환경변수
# ─────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ─────────────────────────────────────────
# 상수
# ─────────────────────────────────────────
KST            = timezone(timedelta(hours=9))
KOSDAQ_150_IDX = "2203"          # pykrx KOSDAQ 150 지수 코드
HIST_DAYS      = 110             # 기술 지표 계산용 과거 일수 (BB 수축 신호에 약 40거래일 필요)
MIN_SCORE      = 4               # 최소 알림 점수 (6점 만점)
MAX_RESULTS    = 10              # 최대 알림 종목 수
MAX_WORKERS    = 8               # 병렬 처리 스레드 수

# ─────────────────────────────────────────
# 유니버스: KOSDAQ 150 종목 수집
# ─────────────────────────────────────────

def get_kosdaq150() -> list[str]:
    """KOSDAQ 150 구성 종목 코드 반환
    - 최근 10일 순차 시도 (주말·공휴일 포함)
    - 폴백: 최근 5일 시가총액 상위 150개
    - 최종 폴백: FinanceDataReader 코스닥 전체 상위 150개
    """
    import FinanceDataReader as fdr

    now = datetime.now(KST)

    # ── 1차: 지수 구성 종목 (최근 10일) ──────
    for delta in range(10):
        date = (now - timedelta(days=delta)).strftime("%Y%m%d")
        try:
            tickers = stock.get_index_portfolio_deposit_file(KOSDAQ_150_IDX, date)
            if tickers is None:
                continue
            # DataFrame
            if isinstance(tickers, pd.DataFrame) and not tickers.empty:
                for col in ("티커", "종목코드", "Code", "code"):
                    if col in tickers.columns:
                        result = tickers[col].astype(str).str.zfill(6).tolist()
                        if result:
                            print(f"KOSDAQ 150 종목 {len(result)}개 로드 ({date})")
                            return result
                # 첫 번째 컬럼 시도
                result = tickers.iloc[:, 0].astype(str).str.zfill(6).tolist()
                if result:
                    print(f"KOSDAQ 150 종목 {len(result)}개 로드 ({date})")
                    return result
            # Series
            elif isinstance(tickers, pd.Series) and not tickers.empty:
                result = tickers.astype(str).str.zfill(6).tolist()
                if result:
                    print(f"KOSDAQ 150 종목 {len(result)}개 로드 ({date})")
                    return result
            # List/iterable
            elif hasattr(tickers, "__iter__"):
                result = [str(t).zfill(6) for t in tickers if str(t).strip()]
                if result:
                    print(f"KOSDAQ 150 종목 {len(result)}개 로드 ({date})")
                    return result
        except Exception as e:
            print(f"[경고] 지수 조회 실패 ({date}): {e}")

    # ── 2차 폴백: 시가총액 상위 150개 (최근 5일) ─
    print("[경고] 지수 조회 실패 → 시가총액 상위 150개로 대체")
    for delta in range(5):
        date = (now - timedelta(days=delta)).strftime("%Y%m%d")
        try:
            df = stock.get_market_cap_by_ticker(date, market="KOSDAQ")
            if df is not None and not df.empty:
                result = df.sort_values("시가총액", ascending=False).head(150).index.tolist()
                if result:
                    print(f"시가총액 상위 150개 로드 ({date})")
                    return result
        except Exception as e:
            print(f"[경고] 시가총액 조회 실패 ({date}): {e}")

    # ── 3차 폴백: FinanceDataReader ──────────
    print("[경고] 시가총액 조회 실패 → FinanceDataReader 코스닥 상위 150개로 대체")
    try:
        listing = fdr.StockListing("KOSDAQ")
        if listing is not None and not listing.empty and "Code" in listing.columns:
            result = listing["Code"].dropna().astype(str).str.zfill(6).head(150).tolist()
            if result:
                print(f"FDR 코스닥 종목 {len(result)}개 로드")
                return result
    except Exception as e:
        print(f"[오류] FDR 조회 실패: {e}")

    return []

# ─────────────────────────────────────────
# OHLCV 수집
# ─────────────────────────────────────────

def get_ohlcv(ticker: str, today: str) -> pd.DataFrame | None:
    """종목 OHLCV (과거 HIST_DAYS일) 반환"""
    from_date = (datetime.strptime(today, "%Y%m%d") - timedelta(days=HIST_DAYS)).strftime("%Y%m%d")
    try:
        df = stock.get_market_ohlcv_by_date(from_date, today, ticker)
        if df is None or len(df) < 45:
            return None
        if df.index[-1].strftime("%Y%m%d") != today:
            return None  # 당일 데이터 미게시 — 전일 기준 오판 방지
        df = df.rename(columns={"시가": "open", "고가": "high", "저가": "low",
                                  "종가": "close", "거래량": "volume"})
        df = df.astype({"open": float, "high": float, "low": float,
                         "close": float, "volume": float})
        return df
    except Exception as e:
        print(f"[경고] {ticker} OHLCV 실패: {e}")
        return None


def wait_for_today_data(today: str, probe: str = "005930",
                        retries: int = 5, wait_sec: int = 60) -> bool:
    """KRX 당일 시세 게시 확인 — 미게시면 재시도 후 False"""
    for attempt in range(retries):
        try:
            df = stock.get_market_ohlcv_by_date(today, today, probe)
            if df is not None and not df.empty and df.index[-1].strftime("%Y%m%d") == today:
                return True
        except Exception as e:
            print(f"[경고] 당일 데이터 확인 실패 ({attempt + 1}/{retries}): {e}")
        if attempt < retries - 1:
            time.sleep(wait_sec)
    return False

# ─────────────────────────────────────────
# 지표 계산
# ─────────────────────────────────────────

def calc_rsi(close: pd.Series, period: int = 14) -> float:
    """RSI 계산 (Wilder 방식)"""
    delta    = close.diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    last_loss = float(avg_loss.iloc[-1])
    if last_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float((100 - 100 / (1 + rs)).iloc[-1])


def calc_macd(close: pd.Series) -> tuple[float, float]:
    """(MACD값, MACD 5일 전 값) 반환"""
    ema12  = close.ewm(span=12, adjust=False).mean()
    ema26  = close.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    return float(macd.iloc[-1]), float(macd.iloc[-6])


def calc_bb_squeeze(close: pd.Series, period: int = 20) -> bool:
    """볼린저밴드 수축 여부: 현재 밴드폭 < 최근 20일 평균 밴드폭"""
    ma  = close.rolling(period).mean()
    std = close.rolling(period).std()
    bw  = ((ma + 2 * std) - (ma - 2 * std)) / ma  # 밴드폭 비율
    if bw.isna().all():
        return False
    bw = bw.dropna()
    return float(bw.iloc[-1]) < float(bw.iloc[-21:-1].mean())

# ─────────────────────────────────────────
# 6가지 사전 신호 점수화
# ─────────────────────────────────────────

def score_prebreak(df: pd.DataFrame) -> tuple[int, list[str]]:
    """
    사전 발굴 신호 점수 계산 (6점 만점)
    반환: (점수, 신호 설명 리스트)
    """
    close  = df["close"]
    high   = df["high"]
    volume = df["volume"]
    score  = 0
    signals: list[str] = []

    # ① 저항선 직전: 20일 고가의 97~102% 구간 (이미 크게 돌파한 종목 제외)
    high_20 = float(high.iloc[-21:-1].max())
    cur     = float(close.iloc[-1])
    if high_20 > 0 and high_20 * 0.97 <= cur <= high_20 * 1.02:
        score += 1
        ratio = cur / high_20 * 100
        signals.append(f"저항선 직전 {ratio:.1f}%")

    # ② 거래량 3일 연속 증가
    if (volume.iloc[-1] > volume.iloc[-2] > volume.iloc[-3]):
        score += 1
        signals.append("거래량 3일 연속 증가")

    # ③ MACD 0선 아래서 상승 수렴 중
    macd_now, macd_5ago = calc_macd(close)
    if macd_now < 0 and macd_now > macd_5ago:
        score += 1
        signals.append(f"MACD 상승 수렴 ({macd_now:.1f}↑)")

    # ④ RSI 45~55 구간 (모멘텀 준비)
    rsi = calc_rsi(close)
    if 45.0 <= rsi <= 55.0:
        score += 1
        signals.append(f"RSI {rsi:.1f} (모멘텀 준비)")

    # ⑤ 볼린저밴드 수축 (변동성 축소)
    if calc_bb_squeeze(close):
        score += 1
        signals.append("볼린저밴드 수축")

    # ⑥ MA5가 MA20에 수렴 (골든크로스 직전)
    ma5  = float(close.rolling(5).mean().iloc[-1])
    ma20 = float(close.rolling(20).mean().iloc[-1])
    ma5_prev  = float(close.rolling(5).mean().iloc[-4])
    ma20_prev = float(close.rolling(20).mean().iloc[-4])
    gap_now  = ma20 - ma5
    gap_prev = ma20_prev - ma5_prev
    if 0 < gap_now < gap_prev and gap_now < ma20 * 0.03:
        score += 1
        signals.append(f"MA5·MA20 수렴 ({gap_now/ma20*100:.1f}%)")

    return score, signals

# ─────────────────────────────────────────
# 종목 분석 (병렬 실행용)
# ─────────────────────────────────────────

def analyze(ticker: str, today: str) -> dict | None:
    df = get_ohlcv(ticker, today)
    if df is None:
        return None

    score, signals = score_prebreak(df)
    if score < MIN_SCORE:
        return None

    try:
        name = stock.get_market_ticker_name(ticker)
    except Exception:
        name = ticker

    close = float(df["close"].iloc[-1])
    prev  = float(df["close"].iloc[-2])
    chg   = (close - prev) / prev * 100 if prev else 0.0

    vol      = float(df["volume"].iloc[-1])
    vol_avg  = float(df["volume"].iloc[-21:-1].mean())
    vol_ratio = vol / vol_avg if vol_avg > 0 else 0.0

    return {
        "ticker":    ticker,
        "name":      name,
        "close":     close,
        "chg":       chg,
        "vol_ratio": vol_ratio,
        "score":     score,
        "signals":   signals,
    }

# ─────────────────────────────────────────
# 텔레그램
# ─────────────────────────────────────────

def build_message(results: list[dict], ts: str) -> str:
    lines = [
        f"🔍 <b>KOSDAQ 사전발굴봇</b> ({ts})",
        "",
        "⏰ <b>실행</b> 15:45 KST | 대상 KOSDAQ 150",
        "📐 <b>기준</b> 6가지 신호 중 4개 이상 동시 충족",
        "   ①저항선 직전(97~102%) ②거래량 3일 연속증가",
        "   ③MACD 0선 하방 수렴 ④RSI 45~55",
        "   ⑤볼린저밴드 수축 ⑥MA5·MA20 수렴",
        "💡 <b>전략</b> 아직 안 터진 종목 미리 포착 → 내일 상승 대비",
        "━━━━━━━━━━━━━━",
        "",
    ]
    for i, r in enumerate(results, 1):
        star = "⭐" * r["score"]
        lines.append(
            f"{i}. <b>{r['name']}</b> ({r['ticker']})  {star}"
        )
        lines.append(
            f"   현재가 {r['close']:,.0f}원 | {r['chg']:+.1f}% | "
            f"거래량 {r['vol_ratio']:.1f}배"
        )
        lines.append(f"   신호: {' / '.join(r['signals'])}")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━")
    lines.append("※ 사전 발굴 참고용 — 투자 판단은 본인 책임")
    return "\n".join(lines)


def send_telegram(message: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[오류] 텔레그램 환경변수 미설정")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message[:4096],
            "parse_mode": "HTML",
        }, timeout=10)
        resp.raise_for_status()
        print("텔레그램 발송 완료")
    except Exception as e:
        print(f"[오류] 텔레그램 발송 실패: {e}")

# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────

def main():
    now   = datetime.now(KST)
    today = now.strftime("%Y%m%d")
    ts    = now.strftime("%m/%d %H:%M")
    print(f"[{ts}] KOSDAQ 150 사전 발굴 스캐너 시작")

    # 휴장일 가드 — 공휴일에 전 거래일 데이터로 발송되는 것 방지
    try:
        if stock.get_nearest_business_day_in_a_week(today) != today:
            print("오늘은 휴장일 — 종료")
            return
    except Exception as e:
        print(f"[경고] 거래일 확인 실패: {e} — 계속 진행")

    # 당일 데이터 게시 대기 (최대 5분)
    if not wait_for_today_data(today):
        send_telegram(f"⚠️ KOSDAQ 사전발굴봇 ({ts})\n당일 시세 미게시로 스캔 보류")
        return

    # 1. KOSDAQ 150 종목 로드
    tickers = get_kosdaq150()
    if not tickers:
        send_telegram("⚠️ KOSDAQ 150 종목 로드 실패")
        return
    print(f"분석 대상: {len(tickers)}종목")

    # 2. 병렬 분석
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze, t, today): t for t in tickers}
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result:
                results.append(result)
            if i % 30 == 0:
                print(f"  진행 {i}/{len(tickers)}")
            time.sleep(0.02)

    # 3. 점수 내림차순 정렬
    results.sort(key=lambda x: (x["score"], x["vol_ratio"]), reverse=True)
    results = results[:MAX_RESULTS]

    print(f"조건 통과: {len(results)}종목")

    # 4. 텔레그램 발송
    if not results:
        send_telegram(f"🔍 KOSDAQ 150 사전 발굴 ({ts})\n조건 충족 종목 없음")
        return

    msg = build_message(results, ts)
    send_telegram(msg)


if __name__ == "__main__":
    main()
