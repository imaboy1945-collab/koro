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
# 2. 돌파매매 조건 검색 함수 (이격도 보조지표 추가)
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
            df_20 = df.iloc[-21:-1] # 어제까지의 20일 데이터
            
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
            
            # ----------------------------------------
            # 🛡️ [조건 4] 보조지표: 20일선 이격도 제한 (신규 추가)
            # ----------------------------------------
            # 오늘을 포함한 최근 20일간의 평균 가격(20일선) 계산
            ma20 = df['Close'].iloc[-20:].mean()
            
            # 현재 주가가 20일 평균선 대비 20% 이상 높다면 고점 과열로 판단하여 필터링
            if today['Close'] > (ma20 * 1.20):
                continue
            
            # 모든 안전 조건을 통과한 찐 돌파 종목만 리스트에 추가
            vol_multiple = today['Volume'] / avg_volume_20
            
            # 이격도 수치도 메시지에 함께 표시되도록 추가
            disparity = (today['Close'] / ma20) * 100
            result_stocks.append(f"• <b>{name}</b> ({code}) | +{change_percent:.2f}% | 거래 {vol_multiple:.1f}배 | 이격도 {disparity:.1f}%")
                
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
        final_message = "🚀 <b>[코스닥 단기 스윙 돌파 포착]</b>\n\n오늘은 안전한 타점의 강력한 돌파 종목이 없습니다."
        
    send_telegram(final_message)
    print("텔레그램 전송 완료!")
