# PM2.5 ETL Pipeline

擷取環境部開放資料 PM2.5 API，進行每日/全量抓取、清洗，並寫入 SQLite。包含：
- API 重試與超時
- 結構化日誌（支援 run_id 與 JSON）
- SQLite 效能優化（WAL、VACUUM/ANALYZE、UPSERT）
- 失敗 Email 通知（可選）

## 目錄

- [環境需求](#1-環境需求)
- [快速開始](#2-快速開始)
- [使用方式](#3-使用方式)
- [工作排程器設定](#4-工作排程器設定)
- [專案結構](#5-專案結構)
- [環境設定](#6-環境設定)
- [日誌系統](#7-日誌系統)
- [SQLite 效能](#8-sqlite-效能)
- [錯誤通知](#9-錯誤通知)

## 1. 環境需求

- Python 3.11+
- Windows / macOS / Linux 皆可

### 安裝套件

```bash
pip install -r requirements.txt
```

**requirements.txt:**
- pandas
- requests
- python-dotenv

## 2. 快速開始

### 步驟 1: 設定環境變數

在專案根目錄建立 `.env` 檔案：

```env
# PM2.5 API 金鑰（必需）
PM25_API_KEY=your_api_key_here

# SMTP 設定（可選，用於錯誤通知）
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASS=your_app_password
SMTP_FROM=your_email@gmail.com
SMTP_TO=recipient@example.com
```

### 步驟 2: 首次載入歷史資料

```cmd
scripts\pm25_etl.bat full
```

### 步驟 3: 設定每日自動更新

將 `scripts\pm25_etl.bat` 加入 Windows 工作排程器，設定每日凌晨 2:00 執行。

## 3. 使用方式

### 單一 Bat 檔案使用方式

**檔案位置：**
- `scripts\pm25_etl.bat` - 主要執行檔案

### 3.1 首次使用 - 載入歷史資料

```cmd
scripts\pm25_etl.bat full
```

**注意事項：**
- 歷史資料載入約5-10分鐘
- 確保網路連線穩定
- 確保有足夠的磁碟空間

### 3.2 日常更新 - 每日資料更新

```cmd
scripts\pm25_etl.bat
```

**預設行為：**
- 不加參數時自動執行每日更新
- 抓取昨日資料並更新資料庫

### 3.3 其他執行方式

**Python 模組方式：**
```bash
# 從專案根目錄執行
python -m etl.run_pipeline

# 帶參數執行
python -m etl.run_pipeline daily    # 只抓取昨日資料
python -m etl.run_pipeline transform # 只執行清洗
python -m etl.run_pipeline load     # 只匯入資料庫
python -m etl.run_pipeline all      # 依序執行（預設）
```

## 4. 工作排程器設定

### 4.1 每日更新排程

**建議設定：**
- **程式路徑：** `pm25/scripts/pm25_etl.bat`
- **工作目錄：** `pm25/scripts`
- **執行時間：** 每日凌晨 2:00
- **參數：** 不填（預設每日更新）
- (`pm25` 替換為在本機 clone repo 的路徑）

### 4.2 歷史資料載入排程

**設定：**
- **程式路徑：** `pm25/scripts/pm25_etl.bat`
- **工作目錄：** `pm25/scripts`
- **參數：** `full`
- **執行時間：** 手動執行或設定為每月一次
- (`pm25` 替換為在本機 clone repo 的路徑）

### 4.3 設定步驟

1. 開啟 Windows 工作排程器（`taskschd.msc`）
2. 建立基本工作
3. 設定觸發程序為「每日」
4. 設定動作為「啟動程式」
5. 指定 bat 檔案路徑和工作目錄

## 5. 專案結構

```
pm25/
├── scripts/
│   ├── pm25_etl.bat          # 主要執行檔案
│   └── simple_email_test.py  # email通知測試
├── etl/                      # ETL 程式碼
│   ├── fetch_pm25_daily.py   # 昨日資料抓取（重試與超時）
│   ├── fetch_pm25_full.py    # 全量歷史抓取（分頁、重試與超時）
│   ├── transform_pm25_data.py # 清洗、去重、型別轉換
│   ├── load_to_sqlite.py     # SQLite 連線、UPSERT、VACUUM/ANALYZE
│   ├── http_client.py        # 共用 requests Session（Retry/Timeout）
│   ├── log_utils.py          # 結構化日誌與 run_id
│   ├── notify.py             # SMTP Email 通知
│   └── run_pipeline.py       # Pipeline 入口（支援 run_id 與錯誤通知）
├── db/
│   ├── pm25.sqlite          # SQLite 資料庫
│   └── schema.sql           # 可重複執行（不清空資料）
├── data/
│   ├── raw/                  # 原始資料
│   └── cleaned/              # 清洗後資料
├── logs/                     # 日誌檔案
├── .env                      # 環境設定檔
└── README.md                 # 本說明檔案
```

## 6. 環境設定

### 6.1 必需設定

**PM25_API_KEY**
- 從環境部開放資料平台申請
- 用於存取 PM2.5 API

### 6.2 可選設定

**SMTP 設定（用於錯誤通知）**

**Gmail 設定範例：**
```env
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASS=your_app_password
SMTP_FROM=your_email@gmail.com
SMTP_TO=recipient@example.com
```

## 7. 日誌系統

### 7.1 日誌檔案位置

- **ETL 日誌：** `logs\etl.log`
- **排程日誌：** `logs\etl_YYYYMMDD.log`

### 7.2 日誌特性

- 預設同時輸出主控台與 `logs/etl.log`（滾動 5MB x 5）
- 每次執行產生 `run_id`，可用於串查一整次流程
- 如需 JSON 格式，將 `setup_logging(json_logs=True)` 設為 True

### 7.3 日誌內容

- 執行時間和狀態
- API 請求回應
- 資料處理統計
- 錯誤訊息和例外

## 8. SQLite 效能

### 8.1 效能優化設定

- 開啟 WAL 模式
- `synchronous=NORMAL`
- `temp_store=MEMORY`
- 匯入後自動 `ANALYZE` 與 `VACUUM`

### 8.2 資料處理

- 使用 `ON CONFLICT(siteid, monitordate) DO UPDATE` 進行 upsert
- 批次處理提升效能
- 自動去重和資料驗證

## 9. 錯誤通知

### 9.1 通知觸發條件

- 任何階段拋出例外時
- 若 `.env` 設定了 SMTP_* 參數

### 9.2 通知內容

- `run_id` - 執行識別碼
- 錯誤訊息
- 開始/結束時間
- 執行模式

### 9.3 測試 Email 功能

```cmd
# 測試基本 Email 功能
python -c "from etl.notify import send_email; send_email('[PM2.5] Test', 'Test message')"
```

