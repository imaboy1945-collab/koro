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
# 2. 돌파매매 조건 검색 함수 (이격도 보조지표 포함)
# ==========================================
def get_breakout_stocks():
    kosdaq_list = fdr.StockListing('KOSDAQ')
    
    end_date = datetime.today()
    start_date = end_date - timedelta(days=60)
    
    result_stocks = []
    
    for idx, row in kosdaq_list.iterrows():
        code = row['Code']
        name = row['Name']
        
        try:
            df = fdr.DataReader(code, start_date, end_date)
            
            if len(df) < 21: 
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
            
            # 리스트 추가 (이격도 수치 포함)
            vol_multiple = today['Volume'] / avg_volume_20
            disparity = (today['Close'] / ma20) * 100
            result_stocks.append(f"• <b>{name}</b> ({code}) | +{change_percent:.2f}% | 거래 {vol_multiple:.1f}배 | 이격도 {disparity:.1f}%")
                
        except Exception:
            continue
            
    return result_stocks

# ==========================================
# 3. 메인 실행부 (메시지 조립 및 전송)
# ==========================================
if __name__ == "__main__":
    print("스캐닝을 시작합니다. 코스닥 전 종목 조회로 약간의 시간이 소요됩니다...")
    
    breakout_stocks = get_breakout_stocks()
    
    # 텔레그램 메시지 상단에 들어갈 스캐너 개요 및 설명 문구 조립
    intro_text = (
        "🚀 <b>[코스닥 단기 스윙 돌파 포착]</b>\n\n"
        "💡 <b>스캐너 조건 개요</b>\n"
        "1. <b>가격:</b> 20일 최고가 돌파 (악성 매물대 돌파)\n"
        "2. <b>수급:</b> 20일 평균 대비 거래량 3배 이상 폭증\n"
        "3. <b>캔들:</b> +7% 이상 장대양봉 (강한 매수세 장악)\n"
        "4. <b>안전:</b> 20일선 이격도 120% 미만 (고점 추격 방지)\n"
        "──────────────────\n\n"
    )
    
    if breakout_stocks:
        final_message = intro_text + "\n".join(breakout_stocks)
    else:
        final_message = intro_text + "오늘은 모든 조건을 완벽하게 만족하는 강력한 돌파 종목이 없습니다."
        
    send_telegram(final_message)
    print("텔레그램 전송 완료!")
