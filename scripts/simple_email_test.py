#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
簡單的 Email 測試腳本
"""

import os
import sys
from datetime import datetime

# 添加專案根目錄到 Python 路徑
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from dotenv import load_dotenv
from etl.notify import send_email

def main():
    print("=" * 60)
    print("Email 發送功能測試")
    print("=" * 60)
    
    # 載入環境變數
    load_dotenv()
    
    # 檢查 SMTP 設定
    host = os.getenv("SMTP_HOST")
    port = os.getenv("SMTP_PORT")
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    sender = os.getenv("SMTP_FROM")
    to = os.getenv("SMTP_TO")
    
    print("\nSMTP 設定檢查:")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  User: {user}")
    print(f"  Password: {'***' if password else 'Not set'}")
    print(f"  From: {sender}")
    print(f"  To: {to}")
    
    # 檢查必要設定
    required_settings = ["SMTP_HOST", "SMTP_PORT", "SMTP_FROM", "SMTP_TO"]
    missing_settings = [setting for setting in required_settings if not os.getenv(setting)]
    
    if missing_settings:
        print(f"\n[錯誤] 缺少必要的 SMTP 設定: {', '.join(missing_settings)}")
        return False
    
    print("\n[資訊] SMTP 設定完整")
    
    # 發送測試郵件
    print("\n[資訊] 正在發送測試郵件...")
    
    subject = "[PM2.5] Email 功能測試"
    body = f"""這是一封來自 PM2.5 ETL Pipeline 的測試郵件。

測試詳情:
- 時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- 系統: Windows
- 目的: Email 功能測試

如果您收到這封郵件，表示 SMTP 設定正確。

---
PM2.5 ETL Pipeline
自動化資料處理系統
"""
    
    try:
        result = send_email(subject, body)
        
        if result:
            print("[成功] 測試郵件發送成功！")
            print("請檢查您的收件匣確認郵件是否收到。")
            return True
        else:
            print("[錯誤] 測試郵件發送失敗")
            print("請檢查以下項目:")
            print("1. SMTP 設定是否正確")
            print("2. 網路連線是否正常")
            print("3. 郵件服務商是否支援 SMTP")
            print("4. 防火牆是否阻擋 SMTP 連線")
            return False
            
    except Exception as e:
        print(f"[錯誤] 發送郵件時發生例外: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n" + "=" * 60)
        print("Email 功能測試完成 - 成功！")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("Email 功能測試完成 - 失敗！")
        print("=" * 60)
    
    input("\n按 Enter 鍵結束...")
    sys.exit(0 if success else 1)
