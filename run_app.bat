@echo off
chcp 65001 >nul
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" winghouse_app.py
) else (
    python winghouse_app.py
)

if errorlevel 1 (
    echo.
    echo 실행 중 오류가 발생했습니다.
    pause
)
