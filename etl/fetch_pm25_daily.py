import requests
import pandas as pd
from datetime import date, timedelta
import os
from dotenv import load_dotenv
import logging
from etl.log_utils import setup_logging
from etl.http_client import build_session

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

DATA_URL = "https://data.moenv.gov.tw/api/v2/aqx_p_322"
API_KEY = os.getenv("PM25_API_KEY")
RAW_DIR = "data/raw"

_session = build_session(total_retries=5, backoff_factor=0.5, timeout=30)

def fetch_pm25_daily_data():
    os.makedirs(RAW_DIR, exist_ok=True)

    if not API_KEY:
        logger.error("Missing environment variable PM25_API_KEY")
        raise RuntimeError("Missing environment variable PM25_API_KEY")

    # 嘗試多個日期策略
    targets = [
        (date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),  # 昨天
        (date.today() - timedelta(days=2)).strftime("%Y-%m-%d"),  # 前天
        (date.today() - timedelta(days=3)).strftime("%Y-%m-%d"),  # 大前天
    ]
    
    today_str = date.today().strftime("%Y%m%d")
    raw_path = os.path.join(RAW_DIR, f"pm25_daily_{today_str}.csv")
    records = []

    # 策略1：精確日期查詢
    for target_date in targets:
        try:
            logger.info(f"嘗試抓取 {target_date} 的資料...")
            url = f"{DATA_URL}?language=zh&api_key={API_KEY}&filters=monitordate,GR,{target_date}&limit=5000"
            res = _session.get_with_timeout(url)
            logger.info(f"API 回應狀態: {res.status_code}")
            
            if res.status_code == 200:
                data = res.json().get("records", [])
                if data:
                    logger.info(f"成功抓取 {len(data)} 筆 {target_date} 資料")
                    records = data
                    break
                else:
                    logger.warning(f"{target_date} 無資料")
            else:
                logger.error(f"API 錯誤 {res.status_code}: {res.text[:200]}")
        except Exception as e:
            logger.exception(f"抓取 {target_date} 時發生錯誤")

    # 策略2：如果精確查詢都失敗，抓最近大量資料
    if not records:
        try:
            logger.info("精確查詢失敗，嘗試抓取最近大量資料...")
            url = f"{DATA_URL}?language=zh&api_key={API_KEY}&limit=5000&sort=monitordate%20desc"
            res = _session.get_with_timeout(url)
            
            if res.status_code == 200:
                data = res.json().get("records", [])
                if data:
                    # 本地過濾最近3天的資料
                    df = pd.DataFrame(data)
                    df['monitordate'] = pd.to_datetime(df['monitordate'], errors='coerce').dt.date
                    target_dates = {pd.to_datetime(t).date() for t in targets}
                    df = df[df['monitordate'].isin(target_dates)]
                    records = df.to_dict(orient="records")
                    logger.info(f"從大量資料中篩選出 {len(records)} 筆最近資料")
        except Exception as e:
            logger.exception("抓取大量資料時發生錯誤")

    if records:
        df = pd.DataFrame(records)
        tmp_path = raw_path + ".tmp"
        df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
        os.replace(tmp_path, raw_path)
        logger.info(f"已儲存 {len(records)} 筆資料到 {raw_path}")
        return raw_path  # 確保回傳檔案路徑
    else:
        logger.error("所有策略都無法取得資料")
        return None

if __name__ == "__main__":
    result = fetch_pm25_daily_data()
    print(f"抓取結果: {result}")