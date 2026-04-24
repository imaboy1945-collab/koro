import os
import FinanceDataReader as fdr
import requests
from datetime import datetime, timedelta

# ==========================================
# 1. 텔레그램 봇 설정 (GitHub Secrets 연동)
# ==========================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

def send_telegram(message):
    """텔레그램 메시지 전송 함수"""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("텔레그램 토큰이나 챗 아이디가 설정되지 않아 전송을 건너뜁니다.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }
    try:
        requests.post(url, data=payload)
    except Exception as e:
        print(f"텔레그램 전송 중 오류 발생: {e}")

# ==========================================
# 2. 신규: 이전 테스트 패턴 확인 함수
# ==========================================
def check_prior_test_pattern(df):
    """
    [조건 5] 최근 30일 이내 +15% 이상 장대양봉 존재 여부 확인
    [조건 6] 그 이후 20일선 ±5% 이내로 되돌아온 적 있는지 확인
    [조건 7] 어제 종가가 20일선 ±5% 이내인지 확인 (수렴 확인)
    """
    if len(df) < 30:
        return False

    # [조건 7] 어제 종가 기준 20일선 ±5% 이내 (오늘 돌파 직전에 수렴 상태였는지)
    yesterday_close = df['Close'].iloc[-2]
    ma20_yesterday = df['Close'].iloc[-21:-1].mean()
    lower_band = ma20_yesterday * 0.95
    upper_band = ma20_yesterday * 1.05

    if not (lower_band <= yesterday_close <= upper_band):
        return False

    # [조건 5] 최근 30일 이내(오늘 제외)에서 +15% 장대양봉 탐색
    # MA20 계산 여유를 위해 최소 20봉 이후부터 탐색
    search_start = max(20, len(df) - 31)
    test_candle_pos = None

    for i in range(search_start, len(df) - 1):  # 오늘(마지막 봉) 제외
        row = df.iloc[i]
        if row['Change'] * 100 >= 15 and row['Close'] > row['Open']:
            test_candle_pos = i
            break  # 가장 오래된 것부터 탐색 (첫 번째 발견)

    if test_candle_pos is None:
        return False

    # [조건 6] 테스트 양봉 이후 ~ 어제까지, 한 번이라도 20일선 ±5% 이내로 되돌아왔는지
    for i in range(test_candle_pos + 1, len(df) - 1):  # 테스트 다음날 ~ 어제
        close = df['Close'].iloc[i]
        ma20 = df['Close'].iloc[i - 20:i].mean()
        if ma20 * 0.95 <= close <= ma20 * 1.05:
            return True  # 되돌림 확인됨

    return False  # 되돌림 없이 계속 고공에 있거나 이탈한 경우

# ==========================================
# 3. 돌파매매 조건 검색 함수 (이격도 보조지표 포함)
# ==========================================
def get_breakout_stocks():
    kosdaq_list = fdr.StockListing('KOSDAQ')

    end_date = datetime.today()
    start_date = end_date - timedelta(days=90)  # 기존 60일 → 90일로 확장 (패턴 탐색 여유분)

    result_stocks = []

    for idx, row in kosdaq_list.iterrows():
        code = row['Code']
        name = row['Name']

        try:
            df = fdr.DataReader(code, start_date, end_date)

            if len(df) < 30:
                continue

            today = df.iloc[-1]
            df_20 = df.iloc[-21:-1]

            # [조건 1] 가격 기준: 20일 최고 종가 돌파
            highest_close_20 = df_20['Close'].max()
            if today['Close'] <= highest_close_20:
                continue

            # [조건 2] 거래량 기준: 20일 평균 대비 300% 폭증
            avg_volume_20 = df_20['Volume'].mean()
            if avg_volume_20 == 0 or today['Volume'] < (avg_volume_20 * 3):
                continue

            # [조건 3] 캔들 기준: 7% 이상 꽉 찬 양봉
            change_percent = today['Change'] * 100
            is_yangbong = today['Close'] > today['Open']
            if not (change_percent >= 7 and is_yangbong):
                continue

            # [조건 4] 보조지표: 20일선 이격도 제한 (상투 방지)
            ma20 = df['Close'].iloc[-20:].mean()
            if today['Close'] > (ma20 * 1.20):
                continue

            # [조건 5,6,7] 신규: 이전 테스트 후 되돌림 패턴 확인
            if not check_prior_test_pattern(df):
                continue

            # 리스트 추가
            vol_multiple = today['Volume'] / avg_volume_20
            disparity = (today['Close'] / ma20) * 100
            result_stocks.append(
                f"• <b>{name}</b> ({code}) | +{change_percent:.2f}% | 거래 {vol_multiple:.1f}배 | 이격도 {disparity:.1f}%"
            )

        except Exception:
            continue

    return result_stocks

# ==========================================
# 4. 메인 실행부 (메시지 조립 및 전송)
# ==========================================
if __name__ == "__main__":
    print("스캐닝을 시작합니다. 코스닥 전 종목 조회로 약간의 시간이 소요됩니다...")

    breakout_stocks = get_breakout_stocks()

    intro_text = (
        "🚀 <b>[코스닥 단기 스윙 돌파 포착]</b>\n\n"
        "💡 <b>스캐너 조건 개요</b>\n"
        "1. <b>가격:</b> 20일 최고가 돌파 (악성 매물대 돌파)\n"
        "2. <b>수급:</b> 20일 평균 대비 거래량 3배 이상 폭증\n"
        "3. <b>캔들:</b> +7% 이상 장대양봉 (강한 매수세 장악)\n"
        "4. <b>안전:</b> 20일선 이격도 120% 미만 (고점 추격 방지)\n"
        "5. <b>패턴:</b> 30일 이내 +15% 테스트 → 20일선 되돌림 → 재돌파\n"
        "──────────────────\n\n"
    )

    if breakout_stocks:
        final_message = intro_text + "\n".join(breakout_stocks)
    else:
        final_message = intro_text + "오늘은 모든 조건을 완벽하게 만족하는 강력한 돌파 종목이 없습니다."

    send_telegram(final_message)
    print("텔레그램 전송 완료!")
