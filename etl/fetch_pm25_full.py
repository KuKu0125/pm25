import requests
import pandas as pd
from datetime import datetime
import os
import time
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

_session = build_session(total_retries=5, backoff_factor=0.5, timeout=60)

def fetch_full_data(limit=5000):
    os.makedirs(RAW_DIR, exist_ok=True)

    if not API_KEY:
        logger.error("Missing environment variable PM25_API_KEY")
        raise RuntimeError("Missing environment variable PM25_API_KEY")

    today = datetime.today().strftime("%Y%m%d")
    raw_path = os.path.join(RAW_DIR, f"pm25_full_{today}.csv")

    all_records = []
    offset = 0

    logger.info("Fetching full historical PM2.5 data via API pagination...")

    try:
        while True:
            logger.info(f"Fetching records with offset={offset}...")
            url = f"{DATA_URL}?language=zh&limit={limit}&offset={offset}&api_key={API_KEY}"
            res = _session.get_with_timeout(url)
            if res.status_code != 200:
                logger.error(f"Bad status={res.status_code} body={res.text[:500]}")
            res.raise_for_status()
            data = res.json().get("records", [])

            if not data:
                logger.info("No more data returned from API.")
                break

            all_records.extend(data)
            logger.info(f"Got {len(data)} records, total so far: {len(all_records)}")

            offset += len(data) 
            time.sleep(0.2)
    except Exception:
        logger.exception("Failed during full PM2.5 data pagination")
        raise

    if all_records:
        df = pd.DataFrame(all_records)
        tmp_path = raw_path + ".tmp"
        df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
        os.replace(tmp_path, raw_path)
        logger.info(f"Saved full data to {raw_path}")
    else:
        logger.warning("No data fetched from API.")

    return raw_path


if __name__ == "__main__":
    fetch_full_data()