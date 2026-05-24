"""
KOSPI 200 + KOSDAQ 150 눌림목 스나이퍼 v3
실전 눌림목 5원칙 기반으로 완전 재설계
"""

import argparse, os, sys, time
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

KST          = timezone(timedelta(hours=9))
BLOG_URL     = os.getenv("BLOG_URL", "https://bestwellth.org")
TOP_N        = int(os.getenv("TOP_N", "10"))
MIN_SCORE    = int(os.getenv("MIN_SCORE", "60"))
MAX_WORKERS  = int(os.getenv("MAX_WORKERS", "8"))
SCAN_LIMIT   = int(os.getenv("SCAN_LIMIT", "0"))

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID")

INDEX_CODES = {
    "kospi":  {"label": "코스피 200", "market": "KOSPI",  "index": "1028",
               "fallback": ["005930","000660","373220","207940","005380"]},
    "kosdaq": {"label": "코스닥 150", "market": "KOSDAQ", "index": "2203",
               "fallback": ["247540","086520","196170","253450","293490"]},
}

@dataclass(frozen=True)
class ScanResult:
    code:        str
    name:        str
    market:      str
    price:       int
    change:      float
    score:       int
    signals:     tuple[str, ...]
    trigger_pct: float
    pullback_pct: float
    days_ago:    int

# ─────────────────────────────────────────
# 인프라
# ─────────────────────────────────────────
def make_session():
    retry = Retry(total=4, backoff_factor=0.6,
                  status_forcelist=(429,500,502,503,504),
                  allowed_methods=("POST",), raise_on_status=False)
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

SESSION = make_session()

def previous_business_dates(days=12):
    now = datetime.now(KST)
    for d in range(days):
        yield (now - timedelta(days=d)).strftime("%Y%m%d")

def normalize_tickers(tickers):
    if tickers is None: return []
    if isinstance(tickers, pd.DataFrame):
        for col in ("티커","종목코드","Code","code"):
            if col in tickers.columns:
                return tickers[col].dropna().astype(str).str.zfill(6).tolist()
        if len(tickers.columns) > 0:
            return tickers.iloc[:,0].dropna().astype(str).str.zfill(6).tolist()
        return []
    if isinstance(tickers, pd.Series):
        return tickers.dropna().astype(str).str.zfill(6).tolist()
    return [str(t).zfill(6) for t in tickers if str(t).strip()]

def get_index_tickers(target):
    config = INDEX_CODES[target]
    for date in previous_business_dates(12):
        try:
            result = normalize_tickers(
                krx.get_index_portfolio_deposit_file(config["index"], date))
            if result:
                print(f"[{config['label']}] {len(result)}종목 ({date})")
                return result
        except: pass
    limit = 200 if target=="kospi" else 150
    for date in previous_business_dates(5):
        try:
            df = krx.get_market_cap_by_ticker(date, market=config["market"])
            if df is not None and not df.empty:
                return df.sort_values("시가총액",ascending=False).head(limit).index.tolist()
        except: pass
    try:
        listing = fdr.StockListing(config["market"])
        if listing is not None and "Code" in listing.columns:
            return listing["Code"].dropna().astype(str).str.zfill(6).head(limit).tolist()
    except: pass
    return config["fallback"]

def get_ohlcv(code):
    end   = datetime.now(KST).strftime("%Y%m%d")
    start = (datetime.now(KST) - timedelta(days=180)).strftime("%Y%m%d")
    try:
        df = krx.get_market_ohlcv_by_date(start, end, code)
        if df is not None and not df.empty:
            return df.rename(columns={"시가":"open","고가":"high","저가":"low",
                                       "종가":"close","거래량":"volume"})
    except: pass
    try:
        df = fdr.DataReader(code,
            (datetime.now(KST)-timedelta(days=180)).strftime("%Y-%m-%d"),
            datetime.now(KST).strftime("%Y-%m-%d"))
        if df is not None and not df.empty:
            return df.rename(columns={"Open":"open","High":"high","Low":"low",
                                       "Close":"close","Volume":"volume"})
    except: pass
    return pd.DataFrame()

def get_stock_name(code):
    try:
        name = krx.get_market_ticker_name(code)
        return name or code
    except: return code

# ─────────────────────────────────────────
# 핵심: 실전 눌림목 5원칙
# ─────────────────────────────────────────

def analyze_ticker(code, market):
    df = get_ohlcv(code)
    if len(df) < 65:   # MA60 계산에 최소 65일 필요
        return None

    close  = df["close"].astype(float)
    high   = df["high"].astype(float)
    low    = df["low"].astype(float)
    open_  = df["open"].astype(float)
    volume = df["volume"].astype(float)

    cur    = float(close.iloc[-1])
    prev   = float(close.iloc[-2])
    change = (cur/prev - 1)*100 if prev else 0.0

    ma10  = float(close.rolling(10).mean().iloc[-1])
    ma20  = float(close.rolling(20).mean().iloc[-1])
    ma60  = float(close.rolling(60).mean().iloc[-1])

    if ma20 == 0 or ma60 == 0:
        return None

    vol_ma20 = float(volume.rolling(20).mean().iloc[-1])

    # ══════════════════════════════════════
    # 원칙 1: 상승 추세 확인 (MA 정배열)
    # 현재가 > MA20 > MA60
    # 이 조건 없으면 하락추세 종목도 걸림
    # ══════════════════════════════════════
    if cur <= ma60:         # 60일선 아래 = 중기 하락추세
        return None
    if ma20 <= ma60:        # MA 역배열 = 추세 꺾임
        return None
    if cur > ma20 * 1.20:   # MA20 이격 20% 초과 = 너무 높이 뜸
        return None

    # ══════════════════════════════════════
    # 원칙 2: 트리거 캔들 탐색 (최근 3~20일)
    # 윗꼬리 없는 장대양봉 + 거래량 동반
    # ══════════════════════════════════════
    trigger_found = False
    for i in range(3, 21):      # 오늘·어제 제외 (눌림 확인 위해 최소 2거래일 후)
        idx = -i
        o = float(open_.iloc[idx])
        c = float(close.iloc[idx])
        h = float(high.iloc[idx])
        l = float(low.iloc[idx])
        v = float(volume.iloc[idx])
        cl = h - l
        if cl == 0 or o == 0: continue

        body       = c - o
        upper_wick = h - c
        body_ratio = body / cl
        wick_ratio = upper_wick / cl
        chg_pct    = body / o * 100

        # 조건: 양봉 + 몸통 60%↑ + 윗꼬리 10%↓ + 상승 3%↑ + 거래량 1.5배↑
        if (body > 0 and
                body_ratio >= 0.60 and
                wick_ratio <= 0.10 and
                chg_pct >= 3.0 and
                v >= vol_ma20 * 1.5):
            trigger_found = True
            days_ago      = i - 1
            trigger_pct   = chg_pct
            trigger_close = c
            trigger_vol   = v
            trigger_idx   = idx
            break

    if not trigger_found:
        return None

    # ══════════════════════════════════════
    # 원칙 3: 눌림 깊이 검증 (3~20%)
    # 너무 얕으면 아직 못 빠진 것
    # 너무 깊으면 추세 이탈 가능성
    # ══════════════════════════════════════
    pullback_pct = (trigger_close - cur) / trigger_close * 100
    if pullback_pct < 3.0:    # 3% 미만: 눌림 아직 충분하지 않음
        return None
    if pullback_pct > 20.0:   # 20% 초과: 추세 이탈 가능성
        return None

    # ══════════════════════════════════════
    # 원칙 4: 눌림 중 거래량 감소 확인
    # 트리거 이후 평균 거래량 < 트리거 거래량
    # 거래량이 줄어야 건강한 눌림, 늘면 분산매도
    # ══════════════════════════════════════
    post_vols = [float(volume.iloc[-j]) for j in range(1, days_ago+1)]
    if not post_vols:
        return None
    post_avg_vol = float(np.mean(post_vols))
    if post_avg_vol >= trigger_vol * 0.8:   # 눌림 중 거래량이 트리거의 80% 이상이면 의심
        return None

    # ══════════════════════════════════════
    # 원칙 5: 지지선 근접 + 오늘 캔들 확인
    # MA10 또는 MA20 ±3% 이내
    # 오늘 종가가 일중 범위의 상위 40% (지지에서 버팀)
    # ══════════════════════════════════════
    near_ma10 = abs(cur - ma10) / ma10 <= 0.03 if ma10 > 0 else False
    near_ma20 = abs(cur - ma20) / ma20 <= 0.03
    if not (near_ma10 or near_ma20):
        return None

    # 오늘 캔들 위치 (종가가 일중 범위의 상위 40% 이상이어야 지지)
    today_range    = float(high.iloc[-1]) - float(low.iloc[-1])
    today_position = (cur - float(low.iloc[-1])) / today_range if today_range > 0 else 0.5
    if today_position < 0.40:
        return None  # 종가가 저점 근처 = 지지 실패

    # 오늘 캔들 윗꼬리 (30% 초과 시 매도 압력)
    today_wick_ratio = (float(high.iloc[-1]) - cur) / today_range if today_range > 0 else 0
    if today_wick_ratio > 0.30:
        return None

    # ══════════════════════════════════════
    # RSI: 눌림목 구간 40~60
    # 과매수(70↑)서 내려온 상태 = 40~60이 이상적
    # ══════════════════════════════════════
    delta    = close.diff()
    ag       = delta.where(delta>0,0.0).ewm(span=14,adjust=False).mean()
    al       = (-delta.where(delta<0,0.0)).ewm(span=14,adjust=False).mean()
    ll       = float(al.iloc[-1])
    rsi_val  = 100.0 if ll==0 else float((100-100/(1+ag/al)).iloc[-1])
    if not (35.0 <= rsi_val <= 65.0):
        return None

    # ══════════════════════════════════════
    # 점수 계산 (100점 만점)
    # ══════════════════════════════════════
    score   = 0
    signals = []

    # 트리거 캔들 품질 (25점)
    if trigger_pct >= 7:   score += 25
    elif trigger_pct >= 5: score += 20
    else:                  score += 12
    trig_vol_x = trigger_vol / vol_ma20
    signals.append(f"{days_ago}일 전 장대양봉 +{trigger_pct:.1f}% (거래량 {trig_vol_x:.1f}배)")

    # 눌림 깊이 (20점) — 3~10% 이상적
    if 3.0 <= pullback_pct <= 10.0:
        score += 20
        signals.append(f"눌림폭 {pullback_pct:.1f}% (이상적)")
    else:
        score += 10
        signals.append(f"눌림폭 {pullback_pct:.1f}%")

    # MA 지지 (25점)
    if near_ma10 and near_ma20:
        score += 25
        signals.append(f"MA10·MA20 동시 지지 ({cur/ma10*100:.1f}%)")
    elif near_ma10:
        score += 20
        signals.append(f"MA10 지지 ({cur/ma10*100:.1f}%)")
    else:
        score += 15
        signals.append(f"MA20 지지 ({cur/ma20*100:.1f}%)")

    # MA 정배열 (15점)
    score += 15
    signals.append(f"MA정배열 (MA20 {ma20:,.0f} > MA60 {ma60:,.0f})")

    # RSI (10점)
    score += 10
    signals.append(f"RSI {rsi_val:.1f} (눌림목 구간)")

    # 오늘 캔들 위치 보너스 (5점)
    if today_position >= 0.60:
        score += 5
        signals.append(f"당일 캔들 상위 {today_position*100:.0f}% 마감")

    if score < MIN_SCORE:
        return None

    return ScanResult(
        code=code, name=get_stock_name(code), market=market,
        price=round(cur), change=round(change, 2),
        score=score, signals=tuple(signals),
        trigger_pct=round(trigger_pct,1),
        pullback_pct=round(pullback_pct,1),
        days_ago=days_ago,
    )

# ─────────────────────────────────────────
# 텔레그램
# ─────────────────────────────────────────

def send_telegram(message, dry_run=False):
    if dry_run:
        print(message); return True
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[error] 텔레그램 환경변수 미설정"); return False
    resp = SESSION.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID,
              "text": message[:4096],
              "parse_mode": "HTML",
              "disable_web_page_preview": True},
        timeout=20)
    ok = resp.status_code == 200
    print("[ok] 전송 완료" if ok else f"[error] {resp.status_code}")
    return ok


def build_report(results, ts):
    header = (
        f"🎯 <b>눌림목 스나이퍼 v3</b>\n"
        f"⏰ {ts} KST | KOSPI 200 + KOSDAQ 150\n"
        "━━━━━━━━━━━━━━\n"
        "📐 <b>5원칙</b>\n"
        "①MA정배열(현재가>MA20>MA60)\n"
        "②윗꼬리없는 장대양봉+거래량(1.5배↑)\n"
        "③눌림깊이 3~20%\n"
        "④눌림중 거래량 감소\n"
        "⑤MA10·MA20 지지 + 오늘 캔들 상위 마감\n"
        "━━━━━━━━━━━━━━\n"
    )
    if not results:
        return header + "\n조건 충족 종목 없음\n"

    lines = [header]
    for i, r in enumerate(results, 1):
        lines.append(f"\n{i}. <b>{r.name}</b> ({r.code}) [{r.market}] <b>{r.score}점</b>")
        lines.append(f"   {r.price:,}원 | {r.change:+.2f}%")
        for s in r.signals:
            lines.append(f"   • {s}")
    lines.append(f"\n━━━━━━━━━━━━━━")
    lines.append(f"🔗 {BLOG_URL}")
    lines.append("※ 투자 판단은 본인 책임")
    return "\n".join(lines)

# ─────────────────────────────────────────
# 스캔
# ─────────────────────────────────────────

def scan_market(target):
    config  = INDEX_CODES[target]
    tickers = get_index_tickers(target)
    if SCAN_LIMIT > 0:
        tickers = tickers[:SCAN_LIMIT]
    print(f"[scan] {config['label']} {len(tickers)}종목")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(analyze_ticker, t, config["label"]): t for t in tickers}
        for idx, f in enumerate(as_completed(futures), 1):
            r = f.result()
            if r: results.append(r)
            if idx % 50 == 0:
                print(f"  진행 {idx}/{len(tickers)} (통과 {len(results)})")
            time.sleep(0.02)

    results.sort(key=lambda r: r.score, reverse=True)
    return results[:TOP_N]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--market", choices=["kospi","kosdaq","all"],
                   default=os.getenv("MARKET","all"))
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main():
    args    = parse_args()
    ts      = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    targets = ["kospi","kosdaq"] if args.market=="all" else [args.market]

    all_results = []
    for t in targets:
        all_results.extend(scan_market(t))
    all_results.sort(key=lambda r: r.score, reverse=True)
    all_results = all_results[:TOP_N]

    report = build_report(all_results, ts)
    send_telegram(report, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
