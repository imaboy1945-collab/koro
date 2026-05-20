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


KST = timezone(timedelta(hours=9))
BLOG_URL = os.getenv("BLOG_URL", "https://bestwellth.org")
TOP_N = int(os.getenv("TOP_N", "10"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "50"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "0"))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID")

INDEX_CODES = {
    "kospi": {
        "label": "코스피 200",
        "market": "KOSPI",
        "index": "1028",
        "fallback": ["005930", "000660", "373220", "207940", "005380"],
    },
    "kosdaq": {
        "label": "코스닥 150",
        "market": "KOSDAQ",
        "index": "2203",
        "fallback": ["247540", "086520", "196170", "253450", "293490"],
    },
}


@dataclass(frozen=True)
class ScanResult:
    code: str
    name: str
    price: int
    change: float
    score: int
    signals: tuple[str, ...]
    volume_ratio: float


def make_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("POST",),
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


SESSION = make_session()


def previous_business_dates(days: int = 12) -> Iterable[str]:
    now = datetime.now(KST)
    for delta in range(days):
        yield (now - timedelta(days=delta)).strftime("%Y%m%d")


def get_index_tickers(target: str) -> list[str]:
    config = INDEX_CODES[target]
    for date_text in previous_business_dates():
        try:
            tickers = krx.get_index_portfolio_deposit_file(config["index"], date_text)
            tickers = normalize_tickers(tickers)
            if tickers:
                return tickers
        except Exception as exc:
            print(f"[warn] {config['label']} 구성종목 조회 실패: {date_text} | {exc}")
    try:
        tickers = krx.get_market_ticker_list(market=config["market"])
        if tickers:
            print(f"[warn] {config['label']} 구성종목 조회 실패, {config['market']} 전체 종목으로 대체합니다.")
            return list(tickers)
    except Exception as exc:
        print(f"[warn] {config['market']} 전체 종목 조회 실패: {exc}")
    try:
        listing = fdr.StockListing(config["market"])
        if listing is not None and not listing.empty and "Code" in listing.columns:
            print(f"[warn] pykrx 조회 실패, FinanceDataReader {config['market']} 종목으로 대체합니다.")
            return listing["Code"].dropna().astype(str).str.zfill(6).tolist()
    except Exception as exc:
        print(f"[warn] FinanceDataReader {config['market']} 종목 조회 실패: {exc}")
    print(f"[warn] {config['label']} 구성종목 조회 실패, 기본 종목으로 대체합니다.")
    return config["fallback"]


def normalize_tickers(tickers: object) -> list[str]:
    if tickers is None:
        return []
    if isinstance(tickers, pd.DataFrame):
        for column in ("티커", "종목코드", "Code", "code"):
            if column in tickers.columns:
                return tickers[column].dropna().astype(str).str.zfill(6).tolist()
        if len(tickers.columns) > 0:
            return tickers.iloc[:, 0].dropna().astype(str).str.zfill(6).tolist()
        return []
    if isinstance(tickers, pd.Series):
        return tickers.dropna().astype(str).str.zfill(6).tolist()
    return [str(ticker).zfill(6) for ticker in tickers if str(ticker).strip()]


def get_stock_name(code: str) -> str:
    try:
        name = krx.get_market_ticker_name(code)
        return name or code
    except Exception:
        pass
    try:
        listing = fdr.StockListing("KRX")
        matched = listing[listing["Code"].astype(str).str.zfill(6) == code]
        if not matched.empty:
            return str(matched.iloc[0]["Name"])
    except Exception:
        pass
    return code


def get_ohlcv(code: str) -> pd.DataFrame:
    end = datetime.now(KST).strftime("%Y%m%d")
    start = (datetime.now(KST) - timedelta(days=420)).strftime("%Y%m%d")
    try:
        df = krx.get_market_ohlcv_by_date(start, end, code)
        if df is not None and not df.empty:
            return df.rename(
                columns={
                    "시가": "open",
                    "고가": "high",
                    "저가": "low",
                    "종가": "close",
                    "거래량": "volume",
                    "등락률": "change",
                }
            )
    except Exception as exc:
        print(f"[warn] pykrx OHLCV 조회 실패: {code} | {exc}")

    try:
        start_date = (datetime.now(KST) - timedelta(days=420)).strftime("%Y-%m-%d")
        end_date = datetime.now(KST).strftime("%Y-%m-%d")
        df = fdr.DataReader(code, start_date, end_date)
        if df is None or df.empty:
            return pd.DataFrame()
        return df.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Change": "change",
            }
        )
    except Exception as exc:
        print(f"[warn] FinanceDataReader OHLCV 조회 실패: {code} | {exc}")
        return pd.DataFrame()


def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def crossed_above(short: pd.Series, long: pd.Series) -> bool:
    return bool(short.iloc[-1] > long.iloc[-1] and short.iloc[-2] <= long.iloc[-2])


def analyze_ticker(code: str) -> ScanResult | None:
    try:
        df = get_ohlcv(code)
        if len(df) < 80:
            return None

        close = df["close"].astype(float)
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        open_ = df["open"].astype(float)
        volume = df["volume"].astype(float)

        latest_close = close.iloc[-1]
        previous_close = close.iloc[-2]
        latest_volume = volume.iloc[-1]
        vol_ma20 = volume.rolling(20).mean()
        vol_ratio = latest_volume / vol_ma20.iloc[-1] if vol_ma20.iloc[-1] > 0 else 0
        body = abs(latest_close - open_.iloc[-1])
        candle_len = max(high.iloc[-1] - low.iloc[-1], 1)
        change = ((latest_close / previous_close) - 1) * 100 if previous_close else 0

        signals: list[str] = []
        score = 0

        if vol_ratio >= 3 and latest_close > open_.iloc[-1] and (body / candle_len) > 0.5:
            score += 25
            signals.append(f"거래량 {vol_ratio:.1f}배+몸통양봉(25)")

        rsi = calc_rsi(close)
        if pd.notna(rsi.iloc[-2]) and rsi.iloc[-2] < 30 <= rsi.iloc[-1] < 45:
            score += 25
            signals.append("RSI 바닥탈출(25)")

        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        if crossed_above(ma5, ma20):
            score += 20
            signals.append("5/20 골든크로스(20)")

        ma20_high = close.iloc[-21:-1].max()
        if latest_close > ma20_high:
            score += 20
            signals.append("20일 신고가 돌파(20)")

        ma60 = close.rolling(60).mean()
        if latest_close > ma20.iloc[-1] > ma60.iloc[-1]:
            score += 10
            signals.append("20/60 상승정렬(10)")

        if score < MIN_SCORE:
            return None

        return ScanResult(
            code=code,
            name=get_stock_name(code),
            price=round(float(latest_close)),
            change=round(float(change), 2),
            score=score,
            signals=tuple(signals),
            volume_ratio=round(float(vol_ratio), 2),
        )
    except Exception as exc:
        print(f"[warn] 분석 실패: {code} | {exc}")
        return None


def send_telegram(message: str, dry_run: bool = False) -> bool:
    if dry_run:
        print(message)
        return True
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[error] TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID 또는 TELEGRAM_TOKEN/CHAT_ID가 설정되지 않았습니다.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message[:4096],
        "disable_web_page_preview": False,
    }
    response = SESSION.post(url, json=payload, timeout=20)
    if response.status_code != 200:
        print(f"[error] 텔레그램 전송 실패: {response.status_code} | {response.text[:300]}")
        return False
    print("[ok] 텔레그램 전송 완료")
    return True


def scan_market(target: str) -> list[ScanResult]:
    config = INDEX_CODES[target]
    tickers = get_index_tickers(target)
    if SCAN_LIMIT > 0:
        tickers = tickers[:SCAN_LIMIT]
    print(f"[scan] {config['label']} 대상 {len(tickers)}개 종목 분석")

    results: list[ScanResult] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(analyze_ticker, code): code for code in tickers}
        for index, future in enumerate(as_completed(future_map), start=1):
            result = future.result()
            if result:
                results.append(result)
            if index % 25 == 0:
                print(f"[scan] {config['label']} 진행 {index}/{len(tickers)}")
            time.sleep(0.03)

    results.sort(key=lambda row: (row.score, row.volume_ratio, row.change), reverse=True)
    return results[:TOP_N]


def build_report(target: str, results: list[ScanResult]) -> str:
    config = INDEX_CODES[target]
    timestamp = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    title = f"📊 [인베스트웰스] {config['label']} 스나이퍼 리포트"

    if not results:
        return (
            f"{title}\n"
            f"기준: {timestamp} KST\n"
            "━━━━━━━━━━━━━━\n\n"
            f"현재 {config['label']} 중 {MIN_SCORE}점 이상 조건을 충족하는 종목이 없습니다.\n\n"
            f"상세 분석: {BLOG_URL}"
        )

    lines = []
    for index, row in enumerate(results, start=1):
        lines.append(
            f"{index}. {row.name} ({row.code}) [{row.score}점]\n"
            f"   신호: {' / '.join(row.signals)}\n"
            f"   등락: {row.change:+.2f}% | 종가: {row.price:,}원"
        )
    return (
        f"{title}\n"
        f"기준: {timestamp} KST\n"
        "━━━━━━━━━━━━━━\n\n"
        + "\n\n".join(lines)
        + f"\n\n상세 분석: {BLOG_URL}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KOSPI/KOSDAQ Telegram stock scanner")
    parser.add_argument("--market", choices=["kospi", "kosdaq", "all"], default=os.getenv("MARKET", "all"))
    parser.add_argument("--dry-run", action="store_true", help="텔레그램 전송 없이 콘솔에만 출력")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    targets = ["kospi", "kosdaq"] if args.market == "all" else [args.market]

    ok = True
    for target in targets:
        results = scan_market(target)
        report = build_report(target, results)
        ok = send_telegram(report, dry_run=args.dry_run) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
