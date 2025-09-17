import pandas as pd
from pathlib import Path
import logging
from etl.log_utils import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def _build_sitename_to_siteid_mapping(df):
    """建立 sitename -> siteid 的對應表（排除空值）"""
    mapping = {}
    valid_data = df[(df['sitename'].notna()) & (df['siteid'].notna()) & (df['sitename'] != '') & (df['siteid'] != '')]
    
    for _, row in valid_data.iterrows():
        sitename = str(row['sitename']).strip()
        siteid = str(row['siteid']).strip()
        if sitename and siteid:
            mapping[sitename] = siteid
    
    logger.info(f"建立 sitename->siteid 對應表，共 {len(mapping)} 個對應關係")
    return mapping

def _fill_missing_siteid(df, sitename_to_siteid):
    """根據 sitename 補完缺失的 siteid"""
    before_fill = df['siteid'].isna().sum()
    
    if before_fill > 0:
        # 找出 siteid 為空但 sitename 不為空的記錄
        mask = df['siteid'].isna() & df['sitename'].notna() & (df['sitename'] != '')
        
        for idx in df[mask].index:
            sitename = str(df.loc[idx, 'sitename']).strip()
            if sitename in sitename_to_siteid:
                df.loc[idx, 'siteid'] = sitename_to_siteid[sitename]
                logger.debug(f"補完 siteid: {sitename} -> {sitename_to_siteid[sitename]}")
        
        after_fill = df['siteid'].isna().sum()
        filled_count = before_fill - after_fill
        logger.info(f"siteid 補完：{before_fill} -> {after_fill}（補完 {filled_count} 筆）")
    
    return df

def transform_pm25_data(raw_dir='data/raw', cleaned_dir='data/cleaned', output_filename='pm25_cleaned.csv'):
    raw_path = Path(raw_dir)
    cleaned_path = Path(cleaned_dir)
    cleaned_path.mkdir(parents=True, exist_ok=True)

    # 1. 讀取並合併所有 CSV
    csv_files = list(raw_path.glob("*.csv"))
    if not csv_files:
        logger.warning("沒有找到任何原始 CSV 檔案")
        return None

    try:
        df_list = [pd.read_csv(file) for file in csv_files]
        df = pd.concat(df_list, ignore_index=True)
        logger.info(f"合併 {len(csv_files)} 個檔案，原始資料筆數：{len(df)}")
    except Exception:
        logger.exception("讀取或合併 CSV 檔案時發生錯誤")
        raise

    # 2. 去重
    before_dedup = len(df)
    df = df.drop_duplicates()
    after_dedup = len(df)
    logger.info(f"去重：{before_dedup} -> {after_dedup}（移除 {before_dedup - after_dedup} 筆）")

    # 3. 建立 sitename -> siteid 對應表（用於補完）
    sitename_to_siteid = _build_sitename_to_siteid_mapping(df)

    # 4. 日期轉換（先正規化字串，再混合格式解析）
    # - 去除前後空白與全形/不換行空白
    # - 統一分隔符為 '-'
    # - 只保留前 10 碼（避免帶時間字串干擾）
    try:
        raw_date_str = (
            df['monitordate']
            .astype(str)
            .str.replace('\u3000|\xa0|\u200b|\ufeff', '', regex=True)
            .str.strip()
        )
        normalized_date_str = (
            raw_date_str
            .str.replace('/', '-', regex=False)
            .str.slice(0, 10)
        )
        # 資料源格式固定為 YYYY-MM-DD（僅有空白差異），用明確格式更穩定
        parsed_datetime = pd.to_datetime(normalized_date_str, errors='coerce', format='%Y-%m-%d')
        df['monitordate'] = parsed_datetime.dt.date

        null_dates = df['monitordate'].isna().sum()
        logger.info(f"日期轉換後缺失：{null_dates} 筆")

        if null_dates > 0:
            # 取樣幾筆無法解析的原始值，方便排查
            bad_samples = raw_date_str[parsed_datetime.isna()].dropna().unique().tolist()[:10]
            logger.warning(f"無法解析的日期樣本（最多 10 筆）：{bad_samples}")
    except Exception:
        logger.exception("日期轉換過程發生非預期錯誤")
        raise

    # 5. 數值轉換
    df['concentration'] = pd.to_numeric(df['concentration'], errors='coerce')
    null_conc = df['concentration'].isna().sum()
    logger.info(f"濃度轉換後缺失：{null_conc} 筆")

    # 6. 補完缺失的 siteid（根據 sitename 對應）
    df = _fill_missing_siteid(df, sitename_to_siteid)

    # 7. 資料品質檢查（移除關鍵欄位缺失）
    before_clean = len(df)
    df = df[~df['siteid'].isna() & ~df['monitordate'].isna()]
    after_clean = len(df)
    logger.info(f"移除關鍵欄位缺失：{before_clean} -> {after_clean}（移除 {before_clean - after_clean} 筆）")

    # 8. 填補必要欄位
    df['sitename'] = df['sitename'].fillna('')

    # 9. 排序並輸出
    df = df.sort_values(by='monitordate')
    
    output_path = cleaned_path / output_filename
    try:
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"資料清理完成，已儲存至：{output_path}")
        return output_path
    except Exception:
        logger.exception("寫出清理後 CSV 檔案時發生錯誤")
        raise

if __name__ == '__main__':
    transform_pm25_data()
