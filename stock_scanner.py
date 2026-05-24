"""
KOSPI 200 + KOSDAQ 150 눌림목 스나이퍼
전략: 윗꼬리 없는 장대양봉 발생 후 첫 눌림목(MA10·MA20) 진입 타점 포착
실행: 15:45 KST (장 마감 확정 종가 기준)
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

import numpy as np
import pandas as pd
import FinanceDataReader as fdr
import requests
from pykrx import stock as krx
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
KST          = timezone(timedelta(hours=9))
BLOG_URL     = os.getenv("BLOG_URL", "https://bestwellth.org")
TOP_N        = int(os.getenv("TOP_N", "10"))
MIN_SCORE    = int(os.getenv("MIN_SCORE", "40"))
MAX_WORKERS  = int(os.getenv("MAX_WORKERS", "8"))
SCAN_LIMIT   = int(os.getenv("SCAN_LIMIT", "0"))

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID")

INDEX_CODES = {
    "kospi": {
        "label":    "코스피 200",
        "market":   "KOSPI",
        "index":    "1028",
        "fallback": ["005930", "000660", "373220", "207940", "005380"],
    },
    "kosdaq": {
        "label":    "코스닥 150",
        "market":   "KOSDAQ",
        "index":    "2203",
        "fallback": ["247540", "086520", "196170", "253450", "293490"],
    },
}

# ─────────────────────────────────────────
# 데이터 수집 인프라
# ─────────────────────────────────────────

@dataclass(frozen=True)
class ScanResult:
    code:         str
    name:         str
    market:       str
    price:        int
    change:       float
    score:        int
    signals:      tuple[str, ...]
    trigger_pct:  float   # 트리거 캔들 상승폭
    days_ago:     int     # 트리거 캔들 몇 일 전
    disparity:    float   # MA20 이격도 (%)


def make_session() -> requests.Session:
    retry = Retry(total=4, connect=4, read=4, status=4,
                  backoff_factor=0.6, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("POST",), raise_on_status=False)
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

SESSION = make_session()


def previous_business_dates(days: int = 12) -> Iterable[str]:
    now = datetime.now(KST)
    for delta in range(days):
        yield (now - timedelta(days=delta)).strftime("%Y%m%d")


def normalize_tickers(tickers) -> list[str]:
    if tickers is None:
        return []
    if isinstance(tickers, pd.DataFrame):
        for col in ("티커", "종목코드", "Code", "code"):
            if col in tickers.columns:
                return tickers[col].dropna().astype(str).str.zfill(6).tolist()
        if len(tickers.columns) > 0:
            return tickers.iloc[:, 0].dropna().astype(str).str.zfill(6).tolist()
        return []
    if isinstance(tickers, pd.Series):
        return tickers.dropna().astype(str).str.zfill(6).tolist()
    return [str(t).zfill(6) for t in tickers if str(t).strip()]


def get_index_tickers(target: str) -> list[str]:
    config = INDEX_CODES[target]

    # 1차: 지수 구성 종목 (최근 12일)
    for date in previous_business_dates(12):
        try:
            tickers = krx.get_index_portfolio_deposit_file(config["index"], date)
            result  = normalize_tickers(tickers)
            if result:
                print(f"[{config['label']}] {len(result)}종목 로드 ({date})")
                return result
        except Exception as e:
            print(f"[warn] {config['label']} 지수 조회 실패: {date} | {e}")

    # 2차: 시가총액 상위 200/150
    print(f"[warn] {config['label']} 지수 조회 실패 → 시가총액 상위 종목으로 대체")
    limit = 200 if target == "kospi" else 150
    for date in previous_business_dates(5):
        try:
            df = krx.get_market_cap_by_ticker(date, market=config["market"])
            if df is not None and not df.empty:
                result = df.sort_values("시가총액", ascending=False).head(limit).index.tolist()
                if result:
                    return result
        except Exception:
            continue

    # 3차: FinanceDataReader
    try:
        listing = fdr.StockListing(config["market"])
        if listing is not None and not listing.empty and "Code" in listing.columns:
            return listing["Code"].dropna().astype(str).str.zfill(6).head(limit).tolist()
    except Exception:
        pass

    print(f"[warn] {config['label']} 조회 실패 → 기본 종목 사용")
    return config["fallback"]


def get_ohlcv(code: str) -> pd.DataFrame:
    end   = datetime.now(KST).strftime("%Y%m%d")
    start = (datetime.now(KST) - timedelta(days=120)).strftime("%Y%m%d")

    try:
        df = krx.get_market_ohlcv_by_date(start, end, code)
        if df is not None and not df.empty:
            return df.rename(columns={"시가": "open", "고가": "high", "저가": "low",
                                       "종가": "close", "거래량": "volume"})
    except Exception as e:
        print(f"[warn] pykrx OHLCV 실패: {code} | {e}")

    try:
        df = fdr.DataReader(code,
                            (datetime.now(KST) - timedelta(days=120)).strftime("%Y-%m-%d"),
                            datetime.now(KST).strftime("%Y-%m-%d"))
        if df is not None and not df.empty:
            return df.rename(columns={"Open": "open", "High": "high", "Low": "low",
                                       "Close": "close", "Volume": "volume"})
    except Exception as e:
        print(f"[warn] FDR OHLCV 실패: {code} | {e}")

    return pd.DataFrame()


def get_stock_name(code: str) -> str:
    try:
        name = krx.get_market_ticker_name(code)
        return name or code
    except Exception:
        return code

# ─────────────────────────────────────────
# 핵심 분석: 눌림목 스나이퍼 전략
# ─────────────────────────────────────────

def find_trigger_candle(df: pd.DataFrame, lookback: int = 10) -> tuple[bool, int, float]:
    """
    최근 lookback일 내 윗꼬리 없는 장대양봉 탐색
    반환: (발견여부, 며칠전, 상승폭%)
    조건:
      - 양봉 (close > open)
      - 몸통 >= 전체 캔들 70% (body_ratio >= 0.7)
      - 윗꼬리 <= 전체 캔들 10% (wick_ratio <= 0.1)
      - 상승폭 >= 3%
    """
    for i in range(2, lookback + 2):   # 오늘([-1]) 제외, 최근 10일
        idx = -(i)
        try:
            o = float(df["open"].iloc[idx])
            c = float(df["close"].iloc[idx])
            h = float(df["high"].iloc[idx])
            l = float(df["low"].iloc[idx])
        except IndexError:
            break

        candle_len = h - l
        if candle_len == 0 or o == 0:
            continue

        body       = c - o
        upper_wick = h - c
        body_ratio = body / candle_len
        wick_ratio = upper_wick / candle_len
        chg_pct    = body / o * 100

        if (body > 0 and            # 양봉
                wick_ratio <= 0.10 and  # 윗꼬리 10% 미만
                body_ratio >= 0.70 and  # 몸통 70% 이상
                chg_pct >= 3.0):        # 최소 3% 상승
            days_ago = i - 1
            return True, days_ago, chg_pct

    return False, 0, 0.0


def analyze_ticker(code: str, market: str) -> ScanResult | None:
    df = get_ohlcv(code)
    if len(df) < 30:
        return None

    close  = df["close"].astype(float)
    high   = df["high"].astype(float)
    low    = df["low"].astype(float)
    open_  = df["open"].astype(float)
    volume = df["volume"].astype(float)

    cur    = float(close.iloc[-1])
    prev   = float(close.iloc[-2])
    change = (cur / prev - 1) * 100 if prev else 0.0

    ma10 = float(close.rolling(10).mean().iloc[-1])
    ma20 = float(close.rolling(20).mean().iloc[-1])

    if ma20 == 0:
        return None

    # ── 필수 조건 ②: MA20 이격도 110% 이하 ──
    disparity = cur / ma20
    if disparity > 1.10:
        return None

    # ── 필수 조건 ①③: 트리거 캔들 + 눌림목 ──
    found, days_ago, trigger_pct = find_trigger_candle(df, lookback=10)
    if not found:
        return None

    # ── 필수 조건 ③: MA10 또는 MA20 ±2% 이내 ──
    near_ma10 = abs(cur - ma10) / ma10 <= 0.02 if ma10 > 0 else False
    near_ma20 = abs(cur - ma20) / ma20 <= 0.02
    if not (near_ma10 or near_ma20):
        return None

    # ── 점수 계산 ──────────────────────────────
    score   = 0
    signals = []

    # 트리거 캔들 품질 (최대 30점)
    if trigger_pct >= 7:
        score += 30
    elif trigger_pct >= 5:
        score += 20
    else:
        score += 10
    signals.append(f"{days_ago}일 전 장대양봉 +{trigger_pct:.1f}%")

    # 눌림목 위치 (최대 20점)
    if near_ma10 and near_ma20:
        score += 20
        pct10 = cur / ma10 * 100
        pct20 = cur / ma20 * 100
        signals.append(f"MA10·MA20 동시 지지 ({pct10:.1f}% / {pct20:.1f}%)")
    elif near_ma10:
        score += 15
        signals.append(f"MA10 지지 ({cur/ma10*100:.1f}%)")
    elif near_ma20:
        score += 10
        signals.append(f"MA20 지지 ({cur/ma20*100:.1f}%)")

    # 이격도 (최대 10점)
    if disparity <= 1.03:
        score += 10
    elif disparity <= 1.07:
        score += 5
    signals.append(f"이격도 {disparity*100:.1f}%")

    # 오늘 캔들도 윗꼬리 없음 보너스 (10점)
    today_body = float(close.iloc[-1]) - float(open_.iloc[-1])
    today_len  = float(high.iloc[-1]) - float(low.iloc[-1])
    if today_len > 0 and today_body > 0:
        today_wick = float(high.iloc[-1]) - float(close.iloc[-1])
        if today_wick / today_len <= 0.10:
            score += 10
            signals.append("당일 캔들도 윗꼬리 없음")

    # 거래량 보너스 (10점)
    vol_ma20  = float(volume.rolling(20).mean().iloc[-1])
    vol_ratio = float(volume.iloc[-1]) / vol_ma20 if vol_ma20 > 0 else 0
    if vol_ratio >= 1.5:
        score += 10
        signals.append(f"거래량 {vol_ratio:.1f}배")

    if score < MIN_SCORE:
        return None

    return ScanResult(
        code=code, name=get_stock_name(code), market=market,
        price=round(cur), change=round(change, 2),
        score=score, signals=tuple(signals),
        trigger_pct=round(trigger_pct, 1), days_ago=days_ago,
        disparity=round(disparity * 100, 1),
    )

# ─────────────────────────────────────────
# 텔레그램
# ─────────────────────────────────────────

def send_telegram(message: str, dry_run: bool = False) -> bool:
    if dry_run:
        print(message)
        return True
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[error] 텔레그램 환경변수 미설정")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    resp = SESSION.post(url, json={
        "chat_id": TELEGRAM_CHAT_ID,
        "text":    message[:4096],
        "disable_web_page_preview": True,
    }, timeout=20)
    if resp.status_code != 200:
        print(f"[error] 텔레그램 전송 실패: {resp.status_code}")
        return False
    print("[ok] 텔레그램 전송 완료")
    return True


def build_report(results: list[ScanResult], ts: str) -> str:
    header_lines = [
        f"🎯 <b>눌림목 스나이퍼</b>",
        "",
        f"⏰ <b>실행</b> 15:45 KST | 대상 KOSPI 200 + KOSDAQ 150",
        "📐 <b>기준</b> 최근 10일 내 윗꼬리 없는 장대양봉 발생",
        "   (몸통 70%↑ · 윗꼬리 10%↓ · 상승 3%↑)",
        "   + 현재가 MA10 또는 MA20 ±2% 이내 눌림",
        "   + MA20 이격도 110% 이하",
        "💡 <b>전략</b> 세력 개입 캔들 확인 후 첫 눌림목 진입",
        "   (윗꼬리=세력 현금화 신호 → 꼬리 없을 때만 유효)",
        f"기준시각: {ts} KST",
        "━━━━━━━━━━━━━━",
    ]
    header = "\n".join(header_lines) + "\n"

    if not results:
        return header + f"\n현재 {MIN_SCORE}점 이상 조건 충족 종목 없음\n\n상세: {BLOG_URL}"

    lines = [header]
    for i, r in enumerate(results, 1):
        star  = "⭐" * min(r.score // 10, 8)
        lines.append(
            f"\n{i}. <b>{r.name}</b> ({r.code}) [{r.market}] {star}"
        )
        lines.append(
            f"   {r.price:,}원 | {r.change:+.1f}% | 점수 {r.score}점"
        )
        for sig in r.signals:
            lines.append(f"   • {sig}")

    lines.append(f"\n━━━━━━━━━━━━━━")
    lines.append(f"상세 분석: {BLOG_URL}")
    lines.append("※ 투자 판단은 본인 책임")
    return "\n".join(lines)

# ─────────────────────────────────────────
# 스캔
# ─────────────────────────────────────────

def scan_market(target: str) -> list[ScanResult]:
    config  = INDEX_CODES[target]
    tickers = get_index_tickers(target)
    if SCAN_LIMIT > 0:
        tickers = tickers[:SCAN_LIMIT]
    print(f"[scan] {config['label']} {len(tickers)}종목 분석 시작")

    results: list[ScanResult] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_ticker, t, config["label"]): t for t in tickers}
        for idx, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result:
                results.append(result)
            if idx % 50 == 0:
                print(f"[scan] {config['label']} 진행 {idx}/{len(tickers)}")
            time.sleep(0.02)

    results.sort(key=lambda r: r.score, reverse=True)
    return results[:TOP_N]

# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market",  choices=["kospi", "kosdaq", "all"],
                        default=os.getenv("MARKET", "all"))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args    = parse_args()
    ts      = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    targets = ["kospi", "kosdaq"] if args.market == "all" else [args.market]

    all_results: list[ScanResult] = []
    for target in targets:
        all_results.extend(scan_market(target))

    all_results.sort(key=lambda r: r.score, reverse=True)
    all_results = all_results[:TOP_N]

    report = build_report(all_results, ts)
    ok     = send_telegram(report, dry_run=args.dry_run)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
