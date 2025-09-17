@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8

cd /d "%~dp0\.."

REM Create necessary directories
if not exist "logs" mkdir "logs"
if not exist "data\raw" mkdir "data\raw"
if not exist "data\cleaned" mkdir "data\cleaned"
if not exist "db" mkdir "db"

REM Check if .env file exists
if not exist ".env" (
    echo [ERROR] .env file not found, please set PM25_API_KEY
    exit /b 1
)

REM Check parameters
if "%1"=="full" (
    echo [INFO] Mode: Loading full historical data
    echo [WARNING] Historical data loading may take several hours
    echo [WARNING] Please ensure stable network connection and sufficient disk space
    echo.
    
    REM Step 1: Fetch full historical data
    echo [INFO] Step 1/3: Fetching full historical data...
    python -c "from etl.fetch_pm25_full import fetch_full_data; fetch_full_data()"
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Historical data fetching failed
        exit /b 1
    )
    
    REM Step 2: Transform data
    echo [INFO] Step 2/3: Transforming data...
    python -c "from etl.transform_pm25_data import transform_pm25_data; transform_pm25_data()"
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Data transformation failed
        exit /b 1
    )
    
    REM Step 3: Load to database
    echo [INFO] Step 3/3: Loading to SQLite database...
    python -c "from etl.load_to_sqlite import load_pm25_to_sqlite; load_pm25_to_sqlite('data/cleaned/pm25_cleaned.csv')"
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Database loading failed
        exit /b 1
    )
    
    echo [SUCCESS] Historical data loading completed
    
) else (
    REM Default mode: Daily update
    echo [INFO] Mode: Daily data update
    
    python -m etl.run_pipeline daily
    if %ERRORLEVEL% EQU 0 (
        echo [SUCCESS] Daily data update completed
    ) else (
        echo [ERROR] Daily data update failed, error code: %ERRORLEVEL%
        exit /b 1
    )
)

REM Show database statistics
echo [INFO] Querying database statistics...
python -c "import sqlite3; conn = sqlite3.connect('db/pm25.sqlite'); cursor = conn.cursor(); cursor.execute('SELECT COUNT(*) FROM pm25'); count = cursor.fetchone()[0]; print(f'Total records: {count:,}'); conn.close()"

echo [COMPLETE] ETL Pipeline finished