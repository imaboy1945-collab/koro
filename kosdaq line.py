import os
import FinanceDataReader as fdr
import requests
from datetime import datetime, timedelta

# ==========================================
# 1. 텔레그램 봇 설정 (GitHub Secrets 연동)
# ==========================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

def send_telegram(message):
    """텔레그램 메시지 전송 함수"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }
    requests.post(url, data=payload)

# ... (이하 기존 get_breakout_stocks() 및 메인 실행부 코드 동일) ...