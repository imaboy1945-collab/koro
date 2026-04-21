import os
import FinanceDataReader as fdr
import requests
from datetime import datetime, timedelta

# ==========================================
# 1. 텔레그램 봇 설정 (GitHub Secrets 연동)
# ==========================================
# 깃허브에 등록된 Secret 이름과 동일하게 맞춰줍니다.
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
# 2. 돌파매매 조건 검색 함수
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
            
            # [조건 1] 가격 기준
            highest_close_20 = df_20['Close'].max()
            if today['Close'] <= highest_close_20:
                continue
                
            # [조건 2] 거래량 기준
            avg_volume_20 = df_20['Volume'].mean()
            if avg_volume_20 == 0 or today['Volume'] < (avg_volume_20 * 3):
                continue
                
            # [조건 3] 캔들 기준
            change_percent = today['Change'] * 100
            is_yangbong = today['Close'] > today['Open']
            
            if change_percent >= 7 and is_yangbong:
                vol_multiple = today['Volume'] / avg_volume_20
                result_stocks.append(f"• <b>{name}</b> ({code}) | +{change_percent:.2f}% | 거래량 {vol_multiple:.1f}배")
                
        except Exception:
            continue
            
    return result_stocks

# ==========================================
# 3. 메인 실행부
# ==========================================
if __name__ == "__main__":
    print("스캐닝을 시작합니다. 코스닥 전 종목 조회로 약간의 시간이 소요됩니다...")
    
    breakout_stocks = get_breakout_stocks()
    
    if breakout_stocks:
        final_message = "🚀 <b>[코스닥 단기 스윙 돌파 포착]</b>\n\n" + "\n".join(breakout_stocks)
    else:
        final_message = "🚀 <b>[코스닥 단기 스윙 돌파 포착]</b>\n\n오늘은 조건에 만족하는 강력한 돌파 종목이 없습니다."
        
    send_telegram(final_message)
    print("텔레그램 전송 완료!")
