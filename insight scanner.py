"""
인사이트스캐너 (통합본)
눌림목 스나이퍼 + CAN SLIM 펀더멘털 합본

필터 흐름:
  기술적 (pykrx)  → 트리거 캔들 + 실제 눌림 + OBV + RSI
  펀더멘털 (Naver) → ROE · PER · 외국인지분율
  투자 가이드      → 피보나치 분할매수 + ATR 손절

실행: 16:10 KST | KOSPI 200 + KOSDAQ 150
"""

import os, sys, time, re, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pykrx import stock as krx
import FinanceDataReader as fdr
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
KST          = timezone(timedelta(hours=9))
BLOG_URL     = os.getenv("BLOG_URL", "https://bestwellth.org")
TOP_N        = int(os.getenv("TOP_N", "8"))
MIN_SCORE    = int(os.getenv("MIN_SCORE", "60"))
MAX_WORKERS  = int(os.getenv("MAX_WORKERS", "6"))

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID")

NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":    "https://finance.naver.com"
}

INDEX_CODES = {
    "kospi":  {"label": "코스피 200", "market": "KOSPI",  "index": "1028",
               "fallback": ["005930","000660","373220","207940","005380"]},
    "kosdaq": {"label": "코스닥 150", "market": "KOSDAQ", "index": "2203",
               "fallback": ["247540","086520","196170","253450","293490"]},
}

# ─────────────────────────────────────────
# 인프라
# ─────────────────────────────────────────
def make_session():
    retry = Retry(total=3, backoff_factor=0.5,
                  status_forcelist=(429,500,502,503,504),
                  allowed_methods=("GET","POST"), raise_on_status=False)
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

SESSION = make_session()


def previous_business_dates(n=12):
    now = datetime.now(KST)
    for d in range(n):
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
                print(f"[{config['label']}] {len(result)}종목 로드 ({date})")
                return result
        except Exception: pass
    limit = 200 if target == "kospi" else 150
    for date in previous_business_dates(5):
        try:
            df = krx.get_market_cap_by_ticker(date, market=config["market"])
            if df is not None and not df.empty:
                return df.sort_values("시가총액", ascending=False).head(limit).index.tolist()
        except Exception: pass
    try:
        listing = fdr.StockListing(config["market"])
        if listing is not None and "Code" in listing.columns:
            return listing["Code"].dropna().astype(str).str.zfill(6).head(limit).tolist()
    except Exception: pass
    return config["fallback"]


def get_ohlcv(code):
    end   = datetime.now(KST).strftime("%Y%m%d")
    start = (datetime.now(KST) - timedelta(days=300)).strftime("%Y%m%d")
    try:
        df = krx.get_market_ohlcv_by_date(start, end, code)
        if df is not None and not df.empty:
            return df.rename(columns={"시가":"open","고가":"high","저가":"low",
                                       "종가":"close","거래량":"volume"})
    except Exception: pass
    try:
        df = fdr.DataReader(
            code,
            (datetime.now(KST) - timedelta(days=300)).strftime("%Y-%m-%d"),
            datetime.now(KST).strftime("%Y-%m-%d"))
        if df is not None and not df.empty:
            return df.rename(columns={"Open":"open","High":"high","Low":"low",
                                       "Close":"close","Volume":"volume"})
    except Exception: pass
    return pd.DataFrame()


def wait_for_today_data(today, probe="005930", retries=5, wait_sec=60):
    """KRX 당일 시세 게시 확인 — 미게시면 재시도 후 False"""
    for attempt in range(retries):
        try:
            df = krx.get_market_ohlcv_by_date(today, today, probe)
            if df is not None and not df.empty and df.index[-1].strftime("%Y%m%d") == today:
                return True
        except Exception as e:
            print(f"[warn] 당일 데이터 확인 실패 ({attempt + 1}/{retries}): {e}")
        if attempt < retries - 1:
            time.sleep(wait_sec)
    return False


def get_stock_name(code):
    try:
        return krx.get_market_ticker_name(code) or code
    except Exception: return code

# ─────────────────────────────────────────
# Naver Finance 펀더멘털
# ─────────────────────────────────────────
def get_naver_fundamental(code):
    url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
    try:
        r = SESSION.get(url, headers=NAVER_HEADERS, timeout=8)
        if r.status_code != 200: return {}
        soup   = BeautifulSoup(r.text, "html.parser")
        tables = soup.find_all("table")
        result = {}

        # EPS, BPS, PER (테이블0)
        t0     = tables[0].get_text(strip=True) if tables else ""
        eps_m  = re.search(r"EPS([\-\d,]+)", t0)
        bps_m  = re.search(r"BPS([\d,]+)", t0)
        per_m  = re.search(r"PER([\d\.]+)", t0)
        if eps_m:
            token = eps_m.group(1)
            raw   = re.sub(r"[^\d]", "", token)
            val   = int(raw) if raw else 0
            result["eps"] = -val if token.lstrip().startswith("-") else val
        if bps_m: result["bps"] = int(bps_m.group(1).replace(",",""))
        if per_m: result["per"] = float(per_m.group(1))
        if result.get("eps",0) > 0 and result.get("bps",0) > 0:
            result["roe"] = round(result["eps"] / result["bps"] * 100, 1)

        # 외국인 지분율 (테이블1)
        if len(tables) > 1:
            lines = [l.strip() for l in tables[1].get_text().split("\n") if l.strip()]
            for i, line in enumerate(lines):
                if "외국인지분율" in line:
                    for nxt in lines[i+1:i+4]:
                        m = re.search(r"^([\d\.]+)%$", nxt)
                        if m:
                            result["foreign_ratio"] = float(m.group(1))
                            break
        return result
    except Exception as e:
        print(f"[Naver] {code} 실패: {e}")
        return {}

# ─────────────────────────────────────────
# 기술적 지표
# ─────────────────────────────────────────
def calc_obv(close, volume):
    obv = [0.0]
    for i in range(1, len(close)):
        if   close.iloc[i] > close.iloc[i-1]: obv.append(obv[-1] + volume.iloc[i])
        elif close.iloc[i] < close.iloc[i-1]: obv.append(obv[-1] - volume.iloc[i])
        else:                                  obv.append(obv[-1])
    return pd.Series(obv, index=close.index)


def calc_rsi(close, period=14):
    """RSI 계산 (Wilder 방식)"""
    delta = close.diff()
    ag    = delta.where(delta>0, 0.0).ewm(alpha=1/period, adjust=False).mean()
    al    = (-delta.where(delta<0, 0.0)).ewm(alpha=1/period, adjust=False).mean()
    ll    = float(al.iloc[-1])
    if ll == 0: return 100.0
    return float((100 - 100/(1+ag/al)).iloc[-1])


def calc_atr(high, low, close, period=14):
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()  # Wilder ATR


def check_rsi_divergence(close, period=14, lookback=20):
    if len(close) < lookback + period: return False
    delta    = close.diff()
    ag       = delta.where(delta>0, 0.0).ewm(alpha=1/period, adjust=False).mean()
    al       = (-delta.where(delta<0, 0.0)).ewm(alpha=1/period, adjust=False).mean()
    rsi_s    = (100 - 100/(1+ag/al.replace(0, np.nan))).fillna(100)
    pr       = close.iloc[-lookback:]
    low1_idx = pr.idxmin()
    low1_val = pr.min()
    before   = pr.loc[:low1_idx].iloc[:-1]
    if len(before) < 3: return False
    low2_idx = before.idxmin()
    low2_val = before.min()
    return low1_val < low2_val and rsi_s.loc[low1_idx] > rsi_s.loc[low2_idx]


def find_trigger_candle(close, high, low, open_, volume, vol_ma20, lookback=20):
    """
    최근 lookback일 내 윗꼬리 없는 장대양봉 탐색
    조건: 양봉 + 몸통 60%↑ + 윗꼬리 10%↓ + 상승 3%↑ + 거래량 1.5배↑
    """
    for i in range(3, lookback + 2):
        idx = -i
        try:
            o,c,h,l,v = (float(open_.iloc[idx]), float(close.iloc[idx]),
                         float(high.iloc[idx]),  float(low.iloc[idx]),
                         float(volume.iloc[idx]))
        except IndexError: break
        cl = h - l
        if cl == 0 or o == 0: continue
        body = c - o
        if (body > 0 and
                (h-c)/cl <= 0.10 and
                body/cl   >= 0.60 and
                body/o*100 >= 3.0 and
                v >= vol_ma20 * 1.5):
            return True, i-1, body/o*100, c, v
    return False, 0, 0.0, 0.0, 0.0


def calc_fib_levels(recent_high, recent_low):
    """스윙 고점→저점 기준 피보나치 되돌림 레벨"""
    up   = recent_high - recent_low
    f236 = round(recent_high - up * 0.236, -1)
    f382 = round(recent_high - up * 0.382, -1)
    f500 = round(recent_high - up * 0.500, -1)
    f618 = round(recent_high - up * 0.618, -1)
    return f236, f382, f500, f618


def calc_fib_targets(cur, recent_high, recent_low):
    """피보나치 확장 목표가 (1:1, 1.618)"""
    move    = recent_high - recent_low
    if move <= 0: return round(cur*1.05,-1), round(cur*1.08,-1)
    return round(recent_high + move, -1), round(recent_high + move*1.618, -1)

# ─────────────────────────────────────────
# 종목 분석 (기술적 필터)
# ─────────────────────────────────────────
def analyze_ticker(code, market_label, today):
    df = get_ohlcv(code)
    if len(df) < 130: return None
    if df.index[-1].strftime("%Y%m%d") != today:
        return None  # 당일 데이터 미게시 — 전일 기준 오판 방지

    close  = df["close"].astype(float)
    high   = df["high"].astype(float)
    low    = df["low"].astype(float)
    open_  = df["open"].astype(float)
    volume = df["volume"].astype(float)

    cur  = float(close.iloc[-1])
    prev = float(close.iloc[-2])
    chg  = (cur/prev-1)*100 if prev else 0.0

    ma20     = float(close.rolling(20).mean().iloc[-1])
    ma60     = float(close.rolling(60).mean().iloc[-1])
    ma120    = float(close.rolling(120).mean().iloc[-1])
    vol_ma20 = float(volume.rolling(20).mean().iloc[-1])

    if ma20 == 0 or ma60 == 0 or ma120 == 0: return None

    # ══════════════════════════════════════
    # 필수 조건 1: 장기 + 중기 추세 정배열
    # MA120 위 + MA20 > MA60
    # ══════════════════════════════════════
    if cur <= ma120:    return None   # 120일선 아래 = 장기 하락
    if ma20 <= ma60:    return None   # 역배열 = 중기 추세 꺾임
    if cur > ma120*1.30: return None  # 이격 과도 = 고점 추격

    # ══════════════════════════════════════
    # 필수 조건 2: 트리거 캔들 (눌림목 기준점)
    # 최근 2~20일 내 윗꼬리 없는 장대양봉
    # ══════════════════════════════════════
    found, days_ago, trig_pct, trig_close, trig_vol = \
        find_trigger_candle(close, high, low, open_, volume, vol_ma20, lookback=20)
    if not found: return None

    # ══════════════════════════════════════
    # 필수 조건 3: 실제 눌림 확인 (3~20%)
    # ══════════════════════════════════════
    pullback_pct = (trig_close - cur) / trig_close * 100
    if not (3.0 <= pullback_pct <= 20.0): return None

    # ══════════════════════════════════════
    # 필수 조건 4: 눌림 중 거래량 감소
    # 세력 매집 → 눌림 중엔 거래량 줄어야 정상
    # ══════════════════════════════════════
    post_vols = [float(volume.iloc[-j]) for j in range(1, days_ago+1)]
    if not post_vols: return None
    if np.mean(post_vols) >= trig_vol * 0.80: return None

    # ══════════════════════════════════════
    # 필수 조건 5: MA10 또는 MA20 ±3% 지지
    # ══════════════════════════════════════
    ma10     = float(close.rolling(10).mean().iloc[-1])
    near_ma10 = abs(cur-ma10)/ma10 <= 0.03 if ma10>0 else False
    near_ma20 = abs(cur-ma20)/ma20 <= 0.03
    if not (near_ma10 or near_ma20): return None

    # 오늘 캔들 일중 위치 ≥ 40% (지지에서 버팀)
    t_range = float(high.iloc[-1]) - float(low.iloc[-1])
    t_pos   = (cur - float(low.iloc[-1])) / t_range if t_range > 0 else 0.5
    if t_pos < 0.40: return None

    # 오늘 캔들 윗꼬리 ≤ 30%
    t_wick = (float(high.iloc[-1]) - cur) / t_range if t_range > 0 else 0
    if t_wick > 0.30: return None

    # ══════════════════════════════════════
    # 필수 조건 6: OBV 스마트머니 유입
    # ══════════════════════════════════════
    obv     = calc_obv(close, volume)
    obv_ema = obv.ewm(span=20, adjust=False).mean()
    if float(obv.iloc[-1]) <= float(obv_ema.iloc[-1]): return None

    # ══════════════════════════════════════
    # 필수 조건 7: RSI 눌림목 구간 (35~65)
    # ══════════════════════════════════════
    rsi_val = calc_rsi(close)
    if not (35.0 <= rsi_val <= 65.0): return None

    rsi_div = check_rsi_divergence(close)

    # ══════════════════════════════════════
    # 리스크 관리 지표 계산
    # ══════════════════════════════════════
    atr_val     = float(calc_atr(high, low, close).iloc[-1])
    stop_2x     = round(cur - 2*atr_val, -1)
    stop_3x     = round(cur - 3*atr_val, -1)

    recent_low  = float(low.iloc[-30:].min())
    recent_high = float(high.iloc[-30:].max())
    if recent_high <= recent_low: return None
    fib_236, fib_382, fib_500, fib_618 = calc_fib_levels(recent_high, recent_low)
    if cur <= fib_618:
        return None  # 61.8% 이탈 = 눌림목 무효 (매매원칙상 전량 손절 구간)
    fib1, fib2  = calc_fib_targets(cur, recent_high, recent_low)

    # ══════════════════════════════════════
    # 점수 계산
    # ══════════════════════════════════════
    score   = 0
    signals = []

    # 트리거 캔들 (25점)
    trig_vol_x = trig_vol / vol_ma20
    if trig_pct >= 7:   score += 25
    elif trig_pct >= 5: score += 18
    else:               score += 10
    signals.append(f"{days_ago}일 전 장대양봉 +{trig_pct:.1f}% (거래량 {trig_vol_x:.1f}배)")

    # 눌림 깊이 (20점) — 3~10%가 이상적
    if 3.0 <= pullback_pct <= 10.0:
        score += 20
        signals.append(f"눌림폭 {pullback_pct:.1f}% (이상적)")
    else:
        score += 10
        signals.append(f"눌림폭 {pullback_pct:.1f}%")

    # MA 지지 (20점)
    if near_ma10 and near_ma20:
        score += 20
        signals.append(f"MA10·MA20 동시 지지 ({cur/ma10*100:.1f}%)")
    elif near_ma10:
        score += 15
        signals.append(f"MA10 지지 ({cur/ma10*100:.1f}%)")
    else:
        score += 10
        signals.append(f"MA20 지지 ({cur/ma20*100:.1f}%)")

    # 장기 추세 (10점)
    score += 10
    signals.append(f"MA정배열 MA120({ma120:,.0f}) · MA20({ma20:,.0f}) · MA60({ma60:,.0f})")

    # OBV (10점)
    score += 10
    obv_chg = (float(obv.iloc[-1])-float(obv.iloc[-5])) / max(abs(float(obv.iloc[-5])),1)*100
    signals.append(f"OBV 스마트머니 유입 ({obv_chg:+.1f}%)")

    # RSI (5점 + 다이버전스 10점)
    score += 5
    rsi_tag = " + 강세 다이버전스🔥" if rsi_div else ""
    signals.append(f"RSI {rsi_val:.1f}{rsi_tag}")
    if rsi_div: score += 10

    return {
        "code": code, "name": get_stock_name(code), "market": market_label,
        "price": cur, "change": chg, "score": score,
        "ma20": ma20, "ma120": ma120,
        "rsi_val": rsi_val, "rsi_div": rsi_div,
        "atr": atr_val, "stop_2x": stop_2x, "stop_3x": stop_3x,
        "fib_236": fib_236, "fib_382": fib_382, "fib_500": fib_500, "fib_618": fib_618,
        "fib1": fib1, "fib2": fib2,
        "signals": signals,
        "roe": 0.0, "per": 0.0, "foreign_ratio": 0.0,
    }

# ─────────────────────────────────────────
# Naver 펀더멘털 보강
# ─────────────────────────────────────────
def enrich_with_naver(r):
    nav   = get_naver_fundamental(r["code"])
    time.sleep(0.3)

    # 적자 기업 제외 (CAN SLIM) — 데이터 확보 시에만 적용
    if nav.get("eps") is not None and nav["eps"] < 0:
        return None

    roe   = nav.get("roe", 0.0)
    per   = nav.get("per", 0.0)
    f_rat = nav.get("foreign_ratio", 0.0)

    r.update({"roe": roe, "per": per, "foreign_ratio": f_rat})

    extra = 0
    fsigs = []

    if roe >= 15:
        extra += 15
        fsigs.append(f"ROE {roe:.1f}% ✅ (CAN SLIM 기준)")
    elif roe >= 10:
        extra += 8
        fsigs.append(f"ROE {roe:.1f}%")
    elif roe > 0:
        fsigs.append(f"ROE {roe:.1f}%")

    if 0 < per <= 20 and roe >= 15:
        extra += 10
        fsigs.append(f"PER {per:.1f} (저평가 성장주)")
    elif 0 < per <= 30:
        fsigs.append(f"PER {per:.1f}")

    if 0 < f_rat <= 35:
        fsigs.append(f"외국인지분 {f_rat:.1f}%")
    elif f_rat > 35:
        fsigs.append(f"외국인지분 {f_rat:.1f}%")

    r["score"]   += extra
    r["signals"]  = r["signals"] + fsigs
    return r

# ─────────────────────────────────────────
# 투자 가이드
# ─────────────────────────────────────────
def build_investment_guide(r):
    """
    눌림목 분할매수 전략
    스윙 고점 기준 피보나치 되돌림 중 현재가 아래 레벨만 매수 사다리로 사용
    61.8% 이탈 시 손절 (이탈 종목은 선별 단계에서 이미 제외)
    """
    price = r["price"]
    fib1  = r["fib1"]
    fib2  = r["fib2"]
    stop  = r["fib_618"]

    candidates = [
        ("23.6% 되돌림", r["fib_236"]),
        ("38.2% 되돌림", r["fib_382"]),
        ("50.0% 되돌림", r["fib_500"]),
    ]
    buys    = [(lab, lv) for lab, lv in candidates if stop < lv < price * 0.995][:2]
    weights = {2: ("34%", "33%", "33%"), 1: ("50%", "50%"), 0: ("100%",)}[len(buys)]

    risk     = abs(price - stop) / price * 100
    last_buy = buys[-1][1] if buys else price
    gap      = (last_buy - stop) / price * 100
    rr       = (fib1 - price) / max(price - stop, 1)

    lines = [
        "   📐 <b>눌림목 분할매수 전략</b>",
        f"   ├ 1차 ({weights[0]}): {price:,.0f}원  ← 지금 진입",
    ]
    for n, (lab, lv) in enumerate(buys, 2):
        lines.append(f"   ├ {n}차 ({weights[n-1]}): {lv:,.0f}원  ← {lab}")
    lines += [
        f"   ├ ── 매수 구간 끝 / 손절 구간 ──",
        f"   ├ 🛑 손절: {stop:,.0f}원  (61.8% 이탈, -{risk:.1f}%)",
        f"   ├    ↳ 마지막 매수~손절 간격 {gap:.1f}%",
        f"   │",
        f"   ├ 🎯 1차 목표: {fib1:,.0f}원  (1:1 확장 — 여기서 절반 익절)",
        f"   ├ 🎯 2차 목표: {fib2:,.0f}원  (1.618 확장)",
        f"   ├ 손익비: {rr:.1f}:1",
        f"   └ 포지션 크기: 단일 손실 총자산 1~2% 이내",
    ]
    return "\n".join(lines)

# ─────────────────────────────────────────
# 텔레그램
# ─────────────────────────────────────────
def build_report(results, ts):
    header = (
        f"🔎 <b>인사이트스캐너</b>\n"
        f"⏰ {ts} KST | KOSPI 200 + KOSDAQ 150\n"
        "━━━━━━━━━━━━━━\n"
        "📐 <b>통합 7대 필터</b>\n"
        "①MA정배열(MA120↑·MA20>MA60)\n"
        "②윗꼬리없는 장대양봉+거래량\n"
        "③눌림 3~20% + 거래량 감소\n"
        "④MA10·MA20 지지 + 오늘 캔들 확인\n"
        "⑤OBV 스마트머니 ⑥RSI 눌림목 구간\n"
        "⑦펀더멘털 적자기업 제외 + ROE·PER 가점\n"
        "━━━━━━━━━━━━━━\n"
    )

    if not results:
        return header + "\n조건 충족 종목 없음\n"

    lines = [header]
    for i, r in enumerate(results, 1):
        lines.append(
            f"\n{i}. <b>{r['name']}</b> ({r['code']}) "
            f"[{r['market']}] <b>{r['score']}점</b>"
        )
        lines.append(f"   {r['price']:,.0f}원 | {r['change']:+.2f}%")
        lines.append("   📊 <b>신호</b>")
        for sig in r["signals"]:
            lines.append(f"   • {sig}")
        lines.append("")
        lines.append(build_investment_guide(r))
        lines.append("")

    lines += [
        "━━━━━━━━━━━━━━",
        "⚠️ <b>매매 원칙</b>",
        "• 1차 진입 후 23.6%·38.2% 추가매수",
        "• 61.8% 이탈 시 전량 손절",
        "• 1차 목표 도달 시 절반 익절",
        "• 단일 종목 최대 손실 총자산 1~2%",
        f"\n🔗 {BLOG_URL}",
    ]
    return "\n".join(lines)


def send_telegram(message, dry_run=False):
    if dry_run:
        print(message); return True
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[error] 환경변수 미설정"); return False
    resp = SESSION.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": message[:4096],
              "parse_mode": "HTML", "disable_web_page_preview": True},
        timeout=20)
    ok = resp.status_code == 200
    print("[ok] 전송 완료" if ok else f"[error] {resp.status_code}")
    return ok

# ─────────────────────────────────────────
# 스캔
# ─────────────────────────────────────────
def scan_market(target, today):
    config  = INDEX_CODES[target]
    tickers = get_index_tickers(target)
    print(f"[scan] {config['label']} {len(tickers)}종목")

    tech_passed = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(analyze_ticker, t, config["label"], today): t for t in tickers}
        for idx, f in enumerate(as_completed(futures), 1):
            r = f.result()
            if r: tech_passed.append(r)
            if idx % 50 == 0:
                print(f"  진행 {idx}/{len(tickers)} (통과 {len(tech_passed)})")
            time.sleep(0.02)

    print(f"기술적 통과 {len(tech_passed)}종목 → Naver 재무 조회")

    enriched = []
    for r in tech_passed:
        r = enrich_with_naver(r)
        if r is None:
            continue  # 적자 기업 제외
        if r["score"] >= MIN_SCORE:
            enriched.append(r)

    enriched.sort(key=lambda x: x["score"], reverse=True)
    return enriched[:TOP_N]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--market", choices=["kospi","kosdaq","all"],
                   default=os.getenv("MARKET","all"))
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main():
    args    = parse_args()
    now     = datetime.now(KST)
    today   = now.strftime("%Y%m%d")
    ts      = now.strftime("%Y-%m-%d %H:%M")
    targets = ["kospi","kosdaq"] if args.market == "all" else [args.market]

    # 휴장일 가드 — 공휴일에 전 거래일 데이터로 발송되는 것 방지
    try:
        if krx.get_nearest_business_day_in_a_week(today) != today:
            print("오늘은 휴장일 — 종료")
            return 0
    except Exception as e:
        print(f"[warn] 거래일 확인 실패: {e} — 계속 진행")

    # 당일 데이터 게시 대기 (최대 5분)
    if not wait_for_today_data(today):
        send_telegram(f"⚠️ 인사이트스캐너 ({ts})\n당일 시세 미게시로 스캔 보류",
                      dry_run=args.dry_run)
        return 0

    all_results = []
    for t in targets:
        all_results.extend(scan_market(t, today))

    all_results.sort(key=lambda x: x["score"], reverse=True)
    all_results = all_results[:TOP_N]

    report = build_report(all_results, ts)
    send_telegram(report, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
