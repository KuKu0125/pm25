CREATE TABLE IF NOT EXISTS pm25 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    siteid TEXT NOT NULL,                    -- 測站代碼
    sitename TEXT NOT NULL,                  -- 測站名稱  
    county TEXT,                             -- 縣市
    itemid TEXT,                             -- 項目代碼
    itemname TEXT,                           -- 項目名稱 (中文)
    itemengname TEXT,                        -- 項目名稱 (英文)
    itemunit TEXT,                           -- 單位 (μg/m³)
    monitordate DATE NOT NULL,               -- 監測日期
    concentration REAL,                      -- PM2.5濃度值
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 資料建立時間
    
    -- 唯一性 同一測站同一日期只能有一筆資料
    UNIQUE(siteid, monitordate),
    
    -- 檢查 確保濃度值為非負數
    CHECK (concentration >= 0 OR concentration IS NULL)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_pm25_date ON pm25(monitordate);
CREATE INDEX IF NOT EXISTS idx_pm25_county_date ON pm25(county, monitordate);

-- 檢視
DROP VIEW IF EXISTS latest_pm25;
CREATE VIEW latest_pm25 AS
SELECT 
    siteid,
    sitename,
    county,
    monitordate,
    concentration
FROM pm25 
WHERE monitordate = (SELECT MAX(monitordate) FROM pm25)
ORDER BY county, sitename;