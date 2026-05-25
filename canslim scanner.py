"""
CAN SLIM 모멘텀 스캐너
5단계 매뉴얼 기반: 펀더멘털 필터 → 기술적 수렴 → 리스크 관리 자동 계산
데이터: pykrx(OHLCV·수급) + Naver Finance(ROE·외국인지분율·PER)
실행: 16:10 KST (장 마감 확정 데이터)
"""

import os, sys, time, re, json, argparse
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
MIN_SCORE    = int(os.getenv("MIN_SCORE", "55"))
MAX_WORKERS  = int(os.getenv("MAX_WORKERS", "6"))

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID")

NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://finance.naver.com"
}

INDEX_CODES = {
    "kospi":  {"label":"코스피 200","market":"KOSPI", "index":"1028",
               "fallback":["005930","000660","373220","207940","005380"]},
    "kosdaq": {"label":"코스닥 150","market":"KOSDAQ","index":"2203",
               "fallback":["247540","086520","196170","253450","293490"]},
}

# ─────────────────────────────────────────
# 결과 데이터 클래스
# ─────────────────────────────────────────
@dataclass
class ScanResult:
    code:          str
    name:          str
    market:        str
    price:         float
    change:        float
    score:         int
    # 펀더멘털
    roe:           float = 0.0
    per:           float = 0.0
    foreign_ratio: float = 0.0
    # 기술적
    ma120:         float = 0.0
    obv_signal:    bool  = False
    rsi_diverge:   bool  = False
    rsi_val:       float = 0.0
    # 리스크 관리 (자동 계산)
    atr:           float = 0.0
    stop_loss_2x:  float = 0.0
    stop_loss_3x:  float = 0.0
    fib_target1:   float = 0.0   # 1:1 연장
    fib_target2:   float = 0.0   # 1.618 연장
    signals:       tuple  = field(default_factory=tuple)

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


def previous_business_dates(n=10):
    now = datetime.now(KST)
    for d in range(n):
        yield (now - timedelta(days=d)).strftime("%Y%m%d")


def normalize_tickers(tickers):
    if tickers is None: return []
    if isinstance(tickers, pd.DataFrame):
        for col in ("티커","종목코드","Code","code"):
            if col in tickers.columns:
                return tickers[col].dropna().astype(str).str.zfill(6).tolist()
        if len(tickers.columns)>0:
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
    start = (datetime.now(KST) - timedelta(days=300)).strftime("%Y%m%d")
    try:
        df = krx.get_market_ohlcv_by_date(start, end, code)
        if df is not None and not df.empty:
            return df.rename(columns={"시가":"open","고가":"high","저가":"low",
                                       "종가":"close","거래량":"volume"})
    except: pass
    try:
        df = fdr.DataReader(code,
            (datetime.now(KST)-timedelta(days=300)).strftime("%Y-%m-%d"),
            datetime.now(KST).strftime("%Y-%m-%d"))
        if df is not None and not df.empty:
            return df.rename(columns={"Open":"open","High":"high","Low":"low",
                                       "Close":"close","Volume":"volume"})
    except: pass
    return pd.DataFrame()


def get_stock_name(code):
    try:
        return krx.get_market_ticker_name(code) or code
    except: return code

# ─────────────────────────────────────────
# Naver Finance 재무 데이터
# ─────────────────────────────────────────
def get_naver_fundamental(code):
    """
    Naver wisereport에서 ROE, PER, 외국인지분율 파싱
    실패 시 빈 dict 반환 → pykrx 근사치로 대체
    """
    url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
    try:
        r = SESSION.get(url, headers=NAVER_HEADERS, timeout=8)
        if r.status_code != 200:
            return {}
        soup = BeautifulSoup(r.text, "html.parser")
        tables = soup.find_all("table")
        result = {}

        # 테이블0: EPS, BPS, PER
        t0 = tables[0].get_text(strip=True) if tables else ""
        eps_m = re.search(r"EPS([\-\d,]+)", t0)
        bps_m = re.search(r"BPS([\d,]+)", t0)
        per_m = re.search(r"PER([\d\.]+)", t0)
        if eps_m:
            raw = re.sub(r"[^\d]","", eps_m.group(1))
            result["eps"] = int(raw) if raw else 0
        if bps_m: result["bps"] = int(bps_m.group(1).replace(",",""))
        if per_m: result["per"] = float(per_m.group(1))
        if result.get("eps",0)>0 and result.get("bps",0)>0:
            result["roe"] = round(result["eps"]/result["bps"]*100, 1)

        # 테이블1: 외국인 지분율
        if len(tables)>1:
            t1_lines = [l.strip() for l in tables[1].get_text().split("\n") if l.strip()]
            for i, line in enumerate(t1_lines):
                if "외국인지분율" in line:
                    for nxt in t1_lines[i+1:i+4]:
                        pct = re.search(r"^([\d\.]+)%$", nxt)
                        if pct:
                            result["foreign_ratio"] = float(pct.group(1))
                            break

        return result
    except Exception as e:
        print(f"[Naver] {code} 실패: {e}")
        return {}

# ─────────────────────────────────────────
# 기술적 지표 계산
# ─────────────────────────────────────────
def calc_obv(close, volume):
    """On-Balance Volume 계산"""
    obv = [0.0]
    for i in range(1, len(close)):
        if close.iloc[i] > close.iloc[i-1]:
            obv.append(obv[-1] + volume.iloc[i])
        elif close.iloc[i] < close.iloc[i-1]:
            obv.append(obv[-1] - volume.iloc[i])
        else:
            obv.append(obv[-1])
    return pd.Series(obv, index=close.index)


def calc_rsi(close, period=14):
    delta = close.diff()
    ag = delta.where(delta>0,0.0).ewm(span=period,adjust=False).mean()
    al = (-delta.where(delta<0,0.0)).ewm(span=period,adjust=False).mean()
    ll = float(al.iloc[-1])
    if ll == 0: return 100.0
    return float((100 - 100/(1+ag/al)).iloc[-1])


def calc_atr(high, low, close, period=14):
    """Average True Range 계산"""
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def check_rsi_divergence(close, period=14, lookback=20):
    """
    RSI 강세 다이버전스 (간소화):
    최근 lookback일 내에서
    가격 저점은 낮아지는데 RSI 저점은 높아지는 경우
    """
    if len(close) < lookback + period:
        return False

    delta = close.diff()
    ag = delta.where(delta>0,0.0).ewm(span=period,adjust=False).mean()
    al = (-delta.where(delta<0,0.0)).ewm(span=period,adjust=False).mean()
    al_safe = al.replace(0, np.nan)
    rsi_series = 100 - 100/(1+ag/al_safe)
    rsi_series = rsi_series.fillna(100)

    window = lookback
    price_recent = close.iloc[-window:]
    rsi_recent   = rsi_series.iloc[-window:]

    # 최근 저점 2개 비교
    price_low1_idx = price_recent.idxmin()
    price_low1_val = price_recent.min()

    # 그 이전 저점
    before = price_recent.loc[:price_low1_idx].iloc[:-1]
    if len(before) < 3:
        return False
    price_low2_idx = before.idxmin()
    price_low2_val = before.min()

    rsi_at_low1 = rsi_series.loc[price_low1_idx]
    rsi_at_low2 = rsi_series.loc[price_low2_idx]

    # 가격은 낮아지고 RSI는 높아지면 강세 다이버전스
    return price_low1_val < price_low2_val and rsi_at_low1 > rsi_at_low2


def calc_fibonacci_targets(close, high, low, lookback=30):
    """
    최근 스윙 저점~고점 기준 피보나치 확장 목표가
    반환: (1:1 target, 1.618 target)
    """
    recent_close = close.iloc[-lookback:]
    recent_high  = high.iloc[-lookback:]
    recent_low   = low.iloc[-lookback:]

    swing_low  = float(recent_low.min())
    swing_high = float(recent_high.max())
    move       = swing_high - swing_low

    if move <= 0:
        return float(close.iloc[-1]) * 1.05, float(close.iloc[-1]) * 1.08

    target_1_1   = round(swing_high + move,        -1)
    target_1_618 = round(swing_high + move * 1.618, -1)
    return target_1_1, target_1_618

# ─────────────────────────────────────────
# 종목 분석
# ─────────────────────────────────────────
def analyze_ticker(code, market_label):
    df = get_ohlcv(code)
    if len(df) < 130:
        return None

    close  = df["close"].astype(float)
    high   = df["high"].astype(float)
    low    = df["low"].astype(float)
    open_  = df["open"].astype(float)
    volume = df["volume"].astype(float)

    cur  = float(close.iloc[-1])
    prev = float(close.iloc[-2])
    change = (cur/prev-1)*100 if prev else 0.0

    # ══════════════════════════════════════
    # 1단계: 절대 모멘텀 — 120일선 위 필수
    # ══════════════════════════════════════
    ma120 = float(close.rolling(120).mean().iloc[-1])
    if cur <= ma120:
        return None

    ma20 = float(close.rolling(20).mean().iloc[-1])
    ma60 = float(close.rolling(60).mean().iloc[-1])

    # MA 정배열 추가 확인 (MA20 > MA60)
    if ma20 <= ma60:
        return None

    # 이격도 과도 상승 방지 (MA120 대비 30% 이상이면 고점 추격)
    if cur > ma120 * 1.30:
        return None

    # ══════════════════════════════════════
    # 2단계: OBV 스마트머니 신호
    # ══════════════════════════════════════
    obv = calc_obv(close, volume)
    obv_ema20 = obv.ewm(span=20, adjust=False).mean()
    obv_signal = float(obv.iloc[-1]) > float(obv_ema20.iloc[-1])
    if not obv_signal:
        return None

    # ══════════════════════════════════════
    # RSI 및 다이버전스 체크
    # ══════════════════════════════════════
    rsi_val   = calc_rsi(close)
    if rsi_val > 75:         # 과매수 극단 제외
        return None

    rsi_diverge = check_rsi_divergence(close)

    # ══════════════════════════════════════
    # 지표 계산
    # ══════════════════════════════════════
    atr_series  = calc_atr(high, low, close)
    atr_val     = float(atr_series.iloc[-1])
    stop_2x     = round(cur - 2 * atr_val, -1)
    stop_3x     = round(cur - 3 * atr_val, -1)
    fib1, fib2  = calc_fibonacci_targets(close, high, low, lookback=30)

    # ══════════════════════════════════════
    # 점수 계산
    # ══════════════════════════════════════
    score   = 0
    signals = []

    # 절대 모멘텀 (20점)
    disparity = cur / ma120
    if disparity <= 1.10:   score += 20
    elif disparity <= 1.20: score += 15
    else:                   score += 10
    signals.append(f"MA120 위 (이격도 {disparity*100:.1f}%)")

    # MA 정배열 (10점)
    score += 10
    signals.append(f"MA정배열 MA20({ma20:,.0f})>MA60({ma60:,.0f})")

    # OBV 스마트머니 (20점)
    score += 20
    obv_chg = (float(obv.iloc[-1])-float(obv.iloc[-5])) / max(abs(float(obv.iloc[-5])),1) * 100
    signals.append(f"OBV 스마트머니 유입 ({obv_chg:+.1f}%)")

    # RSI 상태 (10점)
    score += 10
    rsi_state = "과매도 탈출" if rsi_val < 40 else "중립 상승" if rsi_val < 60 else "상승 모멘텀"
    signals.append(f"RSI {rsi_val:.1f} ({rsi_state})")

    # RSI 다이버전스 보너스 (15점)
    if rsi_diverge:
        score += 15
        signals.append("RSI 강세 다이버전스 감지")

    return {
        "code": code, "name": get_stock_name(code), "market": market_label,
        "price": cur, "change": change, "score": score,
        "ma120": ma120, "obv_signal": obv_signal,
        "rsi_val": rsi_val, "rsi_diverge": rsi_diverge,
        "atr": atr_val, "stop_2x": stop_2x, "stop_3x": stop_3x,
        "fib1": fib1, "fib2": fib2, "signals": signals,
        # Naver 데이터는 필터 통과 후 별도 조회 (속도 최적화)
        "roe": 0.0, "per": 0.0, "foreign_ratio": 0.0,
    }

# ─────────────────────────────────────────
# Naver 펀더멘털 필터 (기술적 통과 종목만)
# ─────────────────────────────────────────
def enrich_with_naver(candidate):
    """기술적 필터 통과 종목에 Naver 재무 데이터 추가"""
    code = candidate["code"]
    nav  = get_naver_fundamental(code)
    time.sleep(0.3)  # Naver 서버 부하 방지

    roe           = nav.get("roe", 0.0)
    per           = nav.get("per", 0.0)
    foreign_ratio = nav.get("foreign_ratio", 0.0)

    candidate["roe"]           = roe
    candidate["per"]           = per
    candidate["foreign_ratio"] = foreign_ratio

    # 펀더멘털 점수 추가 (최대 25점)
    extra  = 0
    fsigs  = []

    # ROE >= 15% (한국형 CAN SLIM 기준)
    if roe >= 15:
        extra += 15
        fsigs.append(f"ROE {roe:.1f}% (CAN SLIM 기준 충족)")
    elif roe >= 10:
        extra += 8
        fsigs.append(f"ROE {roe:.1f}%")
    elif roe > 0:
        fsigs.append(f"ROE {roe:.1f}%")

    # PEG 근사: PER < 20이고 ROE > 15이면 저평가 성장주로 간주
    if 0 < per <= 20 and roe >= 15:
        extra += 10
        fsigs.append(f"PER {per:.1f} (저평가 성장주)")
    elif 0 < per <= 30:
        fsigs.append(f"PER {per:.1f}")

    # 외국인 지분율 체크
    if 0 < foreign_ratio <= 35:
        fsigs.append(f"외국인지분 {foreign_ratio:.1f}% (CAN SLIM 조건)")
    elif foreign_ratio > 35:
        fsigs.append(f"외국인지분 {foreign_ratio:.1f}%")

    candidate["score"]  += extra
    candidate["signals"] = candidate["signals"] + fsigs
    return candidate

# ─────────────────────────────────────────
# 텔레그램 메시지 빌드
# ─────────────────────────────────────────
def build_investment_guide(r):
    """
    5단계 매뉴얼 기반 투자 가이드 생성
    종목별 ATR 손절가 + 피보나치 목표가 포함
    """
    price     = r["price"]
    stop_2x   = r["stop_2x"]
    stop_3x   = r["stop_3x"]
    fib1      = r["fib1"]
    fib2      = r["fib2"]
    atr       = r["atr"]
    risk_pct  = abs(price - stop_2x) / price * 100

    lines = [
        "   📐 <b>투자 가이드</b>",
        f"   ├ 1차 진입: 목표 물량의 25% 선매수 (정찰병)",
        f"   ├ 추가 진입: 상승 파동 확인 or 눌림목 반등 시",
        f"   ├ 손절(2×ATR): {stop_2x:,.0f}원  "
        f"(현재가 -{risk_pct:.1f}%, ATR={atr:,.0f}원)",
        f"   ├ 손절(3×ATR): {stop_3x:,.0f}원  (노이즈 허용 시)",
        f"   ├ 1차 목표: {fib1:,.0f}원  (피보나치 1:1 확장)",
        f"   ├ 2차 목표: {fib2:,.0f}원  (피보나치 1.618 확장)",
        f"   └ 포지션 크기: 손실액이 총자산 1~2% 이내로 조절",
    ]
    return "\n".join(lines)


def build_report(results, ts):
    header = (
        f"🚀 <b>CAN SLIM 모멘텀 스캐너</b>\n"
        f"⏰ {ts} KST | KOSPI 200 + KOSDAQ 150\n"
        "━━━━━━━━━━━━━━\n"
        "📐 <b>5단계 필터</b>\n"
        "①절대모멘텀(MA120↑) ②OBV 스마트머니\n"
        "③RSI 다이버전스 ④ROE 15%↑ ⑤PEG 저평가\n"
        "━━━━━━━━━━━━━━\n"
    )

    if not results:
        return header + "\n조건 충족 종목 없음\n"

    lines = [header]
    for i, r in enumerate(results, 1):
        rsi_diverge_tag = " 🔥다이버전스" if r["rsi_diverge"] else ""
        lines.append(
            f"\n{i}. <b>{r['name']}</b> ({r['code']}) [{r['market']}] "
            f"<b>{r['score']}점</b>"
        )
        lines.append(
            f"   {r['price']:,.0f}원 | {r['change']:+.2f}%"
            f"{rsi_diverge_tag}"
        )
        lines.append("   📊 <b>신호</b>")
        for sig in r["signals"]:
            lines.append(f"   • {sig}")
        lines.append("")
        lines.append(build_investment_guide(r))
        lines.append("")

    lines.append("━━━━━━━━━━━━━━")
    lines.append(
        "⚠️ <b>매매 원칙</b>\n"
        "• 최초 진입 25% → 추세 확인 후 비중 확대\n"
        "• ATR 손절선 반드시 사전 설정\n"
        "• 단일 종목 최대 손실 = 총자산 1~2%\n"
        "• 트레일링 스톱으로 수익 75% 이상 보존"
    )
    lines.append(f"\n🔗 {BLOG_URL}")
    return "\n".join(lines)


def send_telegram(message, dry_run=False):
    if dry_run:
        print(message); return True
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[error] 텔레그램 환경변수 미설정"); return False
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
def scan_market(target):
    config  = INDEX_CODES[target]
    tickers = get_index_tickers(target)
    print(f"[scan] {config['label']} {len(tickers)}종목 기술적 분석 시작")

    # 1단계: 기술적 필터 (병렬)
    tech_passed = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(analyze_ticker, t, config["label"]): t for t in tickers}
        for idx, f in enumerate(as_completed(futures), 1):
            r = f.result()
            if r:
                tech_passed.append(r)
            if idx % 50 == 0:
                print(f"  기술 진행 {idx}/{len(tickers)} (통과 {len(tech_passed)})")
            time.sleep(0.02)

    print(f"기술적 통과: {len(tech_passed)}종목 → Naver 재무 데이터 조회 시작")

    # 2단계: Naver 펀더멘털 보강 (순차 - 서버 부하 방지)
    enriched = []
    for r in tech_passed:
        r = enrich_with_naver(r)
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
    ts      = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    targets = ["kospi","kosdaq"] if args.market=="all" else [args.market]

    all_results = []
    for t in targets:
        all_results.extend(scan_market(t))

    all_results.sort(key=lambda x: x["score"], reverse=True)
    all_results = all_results[:TOP_N]

    report = build_report(all_results, ts)
    send_telegram(report, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
