import os
import FinanceDataReader as fdr
import requests
from datetime import datetime, timedelta

# ==========================================
# 1. 텔레그램 봇 설정 (GitHub Secrets 연동)
# ==========================================
# GitHub Actions의 Secrets에 등록된 값을 환경변수로 불러옵니다.
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

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
    # 코스닥 전 종목 리스트 불러오기
    kosdaq_list = fdr.StockListing('KOSDAQ')
    
    # 최근 데이터를 넉넉히 가져오기 위한 날짜 세팅 (약 두 달 치)
    end_date = datetime.today()
    start_date = end_date - timedelta(days=60)
    
    result_stocks = []
    
    for idx, row in kosdaq_list.iterrows():
        code = row['Code']
        name = row['Name']
        
        try:
            # 개별 종목의 일봉 데이터 조회
            df = fdr.DataReader(code, start_date, end_date)
            
            # 상장한 지 20일이 안 된 신규 상장주는 분석 불가하므로 패스
            if len(df) < 21: 
                continue
            
            # 어제까지의 20일 데이터와 오늘 데이터 분리
            today = df.iloc[-1]
            df_20 = df.iloc[-21:-1] 
            
            # ----------------------------------------
            # [조건 1] 가격 기준: 20일 최고 종가 돌파
            # ----------------------------------------
            highest_close_20 = df_20['Close'].max()
            if today['Close'] <= highest_close_20:
                continue
                
            # ----------------------------------------
            # [조건 2] 거래량 기준: 20일 평균 대비 300% 폭증
            # ----------------------------------------
            avg_volume_20 = df_20['Volume'].mean()
            if avg_volume_20 == 0 or today['Volume'] < (avg_volume_20 * 3):
                continue
                
            # ----------------------------------------
            # [조건 3] 캔들 기준: 7% 이상 꽉 찬 양봉
            # ----------------------------------------
            change_percent = today['Change'] * 100
            is_yangbong = today['Close'] > today['Open'] # 시가보다 종가가 높은 진짜 양봉
            
            if change_percent >= 7 and is_yangbong:
                # 3가지 조건을 모두 통과한 종목만 리스트에 추가
                vol_multiple = today['Volume'] / avg_volume_20
                result_stocks.append(f"• <b>{name}</b> ({code}) | +{change_percent:.2f}% | 거래량 {vol_multiple:.1f}배")
                
        except Exception:
            # 상장폐지, 거래정지 등 예외 발생 시 에러를 뿜지 않고 깔끔하게 다음 종목으로 넘어감
            continue
            
    return result_stocks

# ==========================================
# 3. 메인 실행부
# ==========================================
if __name__ == "__main__":
    print("스캐닝을 시작합니다. 코스닥 전 종목 조회로 약간의 시간이 소요됩니다...")
    
    # 조건에 맞는 종목 찾기
    breakout_stocks = get_breakout_stocks()
    
    # 텔레그램으로 보낼 메시지 조립
    if breakout_stocks:
        final_message = "🚀 <b>[코스닥 단기 스윙 돌파 포착]</b>\n\n" + "\n".join(breakout_stocks)
    else:
        final_message = "🚀 <b>[코스닥 단기 스윙 돌파 포착]</b>\n\n오늘은 조건에 만족하는 강력한 돌파 종목이 없습니다."
        
    # 메시지 전송
    send_telegram(final_message)
    print("텔레그램 전송 완료!")
