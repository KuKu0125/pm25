import sqlite3
import pandas as pd
from pathlib import Path
import logging
from etl.log_utils import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def _prepare_connection(conn: sqlite3.Connection):
    cur = conn.cursor()
    # 效能與耐久性平衡
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.close()

def _vacuum_analyze(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("ANALYZE;")
    cur.execute("VACUUM;")
    cur.close()

def load_pm25_to_sqlite(csv_file, db_path='db/pm25.sqlite'):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        conn = sqlite3.connect(db_path)
        _prepare_connection(conn)
        cursor = conn.cursor()
    except Exception:
        logger.exception("無法連線到 SQLite 資料庫")
        raise

    schema_path = Path("db/schema.sql")
    if schema_path.exists():
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_sql = f.read()
            cursor.executescript(schema_sql)
            logger.info("已套用 schema.sql 結構")
        except Exception:
            logger.exception("套用 schema.sql 時發生錯誤")
            conn.close()
            raise
    else:
        logger.warning("找不到 schema.sql，略過結構初始化")

    try:
        df = pd.read_csv(csv_file)
    except Exception:
        logger.exception(f"讀取清理後 CSV 檔案失敗：{csv_file}")
        conn.close()
        raise

    try:
        # 正規化欄位（避免來源擴充造成欄位遺漏）
        required = ["siteid","sitename","county","itemid","itemname","itemengname","itemunit","monitordate","concentration"]
        for col in required:
            if col not in df.columns:
                df[col] = None

        df['monitordate'] = pd.to_datetime(df['monitordate']).dt.date

        # 批次 upsert
        rows = [
            (
                row.get("siteid"),
                row.get("sitename"),
                row.get("county"),
                row.get("itemid"),
                row.get("itemname"),
                row.get("itemengname"),
                row.get("itemunit"),
                row.get("monitordate"),
                row.get("concentration"),
            )
            for _, row in df.iterrows()
        ]

        cursor.executemany("""
        INSERT INTO pm25 (siteid, sitename, county, itemid, itemname, itemengname, itemunit, monitordate, concentration)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(siteid, monitordate) DO UPDATE SET
          sitename=excluded.sitename,
          county=excluded.county,
          itemid=excluded.itemid,
          itemname=excluded.itemname,
          itemengname=excluded.itemengname,
          itemunit=excluded.itemunit,
          concentration=excluded.concentration
        """, rows)

        inserted = cursor.rowcount if cursor.rowcount is not None else len(rows)
        logger.info(f"Upsert 完成，受影響列數（估計）：{inserted}")
    except Exception:
        logger.exception("匯入資料到 SQLite 時發生錯誤")
        raise
    finally:
        conn.commit()
        try:
            _vacuum_analyze(conn)
            logger.info("已執行 ANALYZE 與 VACUUM")
        except Exception:
            logger.exception("ANALYZE/VACUUM 發生錯誤（可忽略）")
        conn.close()

if __name__ == '__main__':
    cleaned_csv = 'data/cleaned/pm25_cleaned.csv'
    load_pm25_to_sqlite(cleaned_csv)