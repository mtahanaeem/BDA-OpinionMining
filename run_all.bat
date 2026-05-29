@echo off
setlocal enabledelayedexpansion

title Real-Time Opinion Mining Pipeline
color 0B

:: ======================================================================
:: run_all.bat — Full Pipeline Launcher
:: Real-Time Opinion Mining at Scale Using Distributed Processing
:: Team: Taha Naeem | Suleman Ahmad | Adil Hayat | 2025-26
:: ======================================================================

set PRODUCER_SCRIPT=producer.py
set CONSUMER_SCRIPT=consumer_streaming.py
set MLLIB_SCRIPT=train_evaluation_mllib.py
set DASHBOARD_DIR=frontend
set PARQUET_OUTPUT=.\output\processed_sentiment_parquet
set LOG_DIR=.\logs
set VENV_DIR=.venv
set KAFKA_TOPIC=social_sentiment
set KAFKA_SERVER=localhost:9092
set PROJECT_DIR=%~dp0
set WAIT_SECONDS=60

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

:: ---------- Load .env ----------
if exist .env (
    for /f "tokens=1,2 delims==" %%a in (.env) do ( set %%a=%%b )
) else (
    echo  [!] .env not found - create with HF_API_TOKEN=your_token
)

cls
echo.
echo  ^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<
echo.
echo     REAL-TIME OPINION MINING AT SCALE
echo     Distributed Stream Processing Pipeline
echo.
echo     Taha Naeem  ^|  Suleman Ahmad  ^|  Adil Hayat
echo     Big Data Analytics - Final Project  ^|  2025-26
echo.
echo  ^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<
echo.

:: ---------- Prerequisites ----------
echo  [0/6] Checking prerequisites...
where python >nul 2>nul
if %ERRORLEVEL% neq 0 ( echo  [FAIL] Python not found. Install from https://python.org & pause & exit /b 1 )
python --version 2>&1 | findstr "Python"
echo.

:: ---------- Step 1 ----------
echo  [1/6] Setting up Python environment...
if exist "%VENV_DIR%\Scripts\activate" (
    call "%VENV_DIR%\Scripts\activate"
    echo   ^> venv activated
) else (
    echo   ^> creating venv...
    uv venv && call "%VENV_DIR%\Scripts\activate" && uv pip install -r requirements.txt
)
echo.

:: ---------- Step 2 ----------
echo  [2/6] Checking Kafka...
python -c "from kafka import KafkaProducer; p=KafkaProducer(bootstrap_servers='%KAFKA_SERVER%'); p.close(); print('   ^> Kafka OK')" 2>nul || echo "   ^> Kafka unreachable - start Kafka first"
echo.

:: ---------- Step 3 ----------
echo  [3/6] Starting Producer...
start "Kafka Producer" cmd /c "title Kafka Producer && cd /d \"%PROJECT_DIR%\" && call \"%VENV_DIR%\Scripts\activate\" && python \"%PRODUCER_SCRIPT%\""
echo   ^> Producer launched
timeout /t 3 /nobreak >nul
echo.

:: ---------- Step 4 ----------
echo  [4/6] Starting Consumer...
if "%HF_API_TOKEN%"=="" echo   ^> WARNING: HF_API_TOKEN not set
start "Spark Consumer" cmd /c "title Spark Consumer && cd /d \"%PROJECT_DIR%\" && call \"%VENV_DIR%\Scripts\activate\" && python \"%CONSUMER_SCRIPT%\""
echo   ^> Consumer launched
echo.

:: ---------- Step 5 ----------
echo  [5/6] Accumulating data (%WAIT_SECONDS% sec)...
for /l %%i in (%WAIT_SECONDS%,-1,1) do (
    cls
    echo.
    echo  ^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<
    echo     ACCUMULATING  -  %%i seconds left
    echo  ^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<^<
    echo.
    echo     CSV ^> Kafka ^> HF API ^> Parquet
    echo.
    if exist "%PARQUET_OUTPUT%" (
        dir /s /b "%PARQUET_OUTPUT%\*.parquet" 2>nul | find /c ".parquet" >nul && (
            for /f %%c in ('dir /s /b "%PARQUET_OUTPUT%\*.parquet" 2^>nul ^| find /c ".parquet"') do echo     Parquet files: %%c
        )
    ) else ( echo     Waiting for output... )
    timeout /t 1 /nobreak >nul
)

cls
echo.
echo  ================================================
echo     DATA ACCUMULATION COMPLETE
echo  ================================================
echo.
echo  Running MLlib...
call "%VENV_DIR%\Scripts\activate"
python "%MLLIB_SCRIPT%" 2>&1
if %ERRORLEVEL% equ 0 ( echo   ^> MLlib done ) else ( echo   ^> MLlib exit code %ERRORLEVEL% )
echo.

:: ---------- Step 6 ----------
echo  [6/6] Deploying Dashboard...
pushd "%PROJECT_DIR%%DASHBOARD_DIR%"
where node >nul 2>nul || ( echo  [FAIL] Node.js required: https://nodejs.org & pause & exit /b 1 )
if not exist "node_modules" call npm install
call npx vite build >nul 2>&1
if %ERRORLEVEL% equ 0 (
    start "Dashboard" cmd /c "title Dashboard && npx vite preview --port 3000"
) else (
    start "Dashboard" cmd /c "title Dashboard && npx vite --port 3000"
)
timeout /t 4 /nobreak >nul
start http://localhost:3000
popd

echo.
echo  ================================================
echo     ALL SYSTEMS OPERATIONAL
echo  ================================================
echo.
echo   Producer   CSV ^> Kafka
echo   Consumer   Kafka ^> HF ^> Parquet
echo   MLlib      training complete
echo   Dashboard  http://localhost:3000
echo.
echo  Close windows to stop.
echo.
pause
endlocal
