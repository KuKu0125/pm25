from etl.fetch_pm25_daily import fetch_pm25_daily_data
from etl.transform_pm25_data import transform_pm25_data
from etl.load_to_sqlite import load_pm25_to_sqlite
import datetime
import logging
import uuid
import os
from etl.log_utils import setup_logging, set_run_id
from etl.notify import send_email

# 統一設定日誌（避免重複設定）
run_id = str(uuid.uuid4())
os.environ["RUN_ID"] = run_id
setup_logging(log_dir="logs", reset=True)
set_run_id(run_id)
logger = logging.getLogger(__name__)

def run_etl_pipeline(mode="all"):
    started = datetime.datetime.now()
    logger.info(f"ETL 開始 run_id={run_id} mode={mode}")

    cleaned_csv = None
    try:
        if mode in ("daily", "all"):
            result = fetch_pm25_daily_data()
            if result and os.path.exists(result):
                logger.info(f"資料抓取完成：{result}")
            else:
                logger.error("資料抓取失敗，無資料產生")
                return

        if mode in ("transform", "all"):
            cleaned_csv = transform_pm25_data()
            if cleaned_csv and os.path.exists(cleaned_csv):
                logger.info(f"資料清洗完成：{cleaned_csv}")
            else:
                logger.error("資料清洗失敗")
                return

        if mode in ("load", "all"):
            if cleaned_csv and os.path.exists(cleaned_csv):
                load_pm25_to_sqlite(cleaned_csv)
                logger.info("資料匯入完成")
            else:
                logger.warning("找不到清理後檔案，匯入跳過")
    except Exception as e:
        ended = datetime.datetime.now()
        logger.exception("Pipeline 失敗")
        subj = f"[pm25] Pipeline Failed run_id={run_id}"
        body = f"mode={mode}\nstarted={started}\nended={ended}\nerror={repr(e)}\nrun_id={run_id}"
        send_email(subj, body)
        return

    ended = datetime.datetime.now()
    duration = (ended - started).total_seconds()
    logger.info(f"Pipeline 執行完成 耗時={duration:.1f}秒 run_id={run_id}")

if __name__ == "__main__":
    logger.info(f"開始時間：{datetime.datetime.now()}")
    run_etl_pipeline()