@echo off
chcp 65001 > nul
setlocal EnableDelayedExpansion

echo.
echo ============================================================
echo   Winghouse Crawler — PyInstaller 빌드 스크립트
echo ============================================================
echo.

REM ── 1. 가상환경 확인 ─────────────────────────────────────────────────────────
if not exist "venv\Scripts\python.exe" (
    echo [ERROR] venv 폴더를 찾을 수 없습니다.
    echo         이 파일은 프로젝트 루트에서 실행해야 합니다.
    pause & exit /b 1
)

REM ── 2. PyInstaller 설치 확인 및 자동 설치 ────────────────────────────────────
venv\Scripts\pip.exe show pyinstaller >nul 2>&1
if errorlevel 1 (
    echo [INFO] PyInstaller가 없습니다. 설치를 시작합니다...
    venv\Scripts\pip.exe install pyinstaller
    if errorlevel 1 (
        echo [ERROR] PyInstaller 설치 실패
        pause & exit /b 1
    )
    echo [OK] PyInstaller 설치 완료
) else (
    for /f "tokens=2" %%v in ('venv\Scripts\pip.exe show pyinstaller ^| findstr "^Version"') do (
        echo [OK] PyInstaller %%v 이미 설치됨
    )
)

REM ── 3. UPX 안내 (선택사항) ───────────────────────────────────────────────────
where upx >nul 2>&1
if errorlevel 1 (
    echo [INFO] UPX가 설치되지 않았습니다. (선택사항 — exe 압축용)
    echo        설치 방법: https://upx.github.io/ 에서 다운로드 후 PATH에 추가
    echo        빌드는 UPX 없이 계속 진행합니다.
    echo.
)

REM ── 4. 이전 빌드 정리 ────────────────────────────────────────────────────────
echo [STEP] 이전 빌드 파일 정리 중...
if exist "build" rmdir /s /q "build"
if exist "dist\Winghouse_Crawler.exe" del /f "dist\Winghouse_Crawler.exe"

REM ── 5. PyInstaller 빌드 실행 ──────────────────────────────────────────────────
echo.
echo [STEP] 빌드 시작... (수 분 소요될 수 있습니다)
echo.

venv\Scripts\pyinstaller.exe ^
    --onefile ^
    --noconsole ^
    --name "Winghouse_Crawler" ^
    --runtime-hook runtime_hook_playwright.py ^
    --hidden-import supabase ^
    --hidden-import postgrest ^
    --hidden-import gotrue ^
    --hidden-import realtime ^
    --hidden-import storage3 ^
    --hidden-import httpx ^
    --hidden-import httpcore ^
    --hidden-import "httpcore._backends.asyncio" ^
    --hidden-import h11 ^
    --hidden-import certifi ^
    --hidden-import anyio ^
    --hidden-import "anyio._backends._asyncio" ^
    --hidden-import sniffio ^
    --hidden-import playwright ^
    --hidden-import "playwright.async_api" ^
    --hidden-import "playwright._impl._api_types" ^
    --hidden-import dotenv ^
    --hidden-import "PyQt6.QtWidgets" ^
    --hidden-import "PyQt6.QtCore" ^
    --hidden-import "PyQt6.QtGui" ^
    --hidden-import "PyQt6.sip" ^
    --exclude-module tkinter ^
    --exclude-module matplotlib ^
    --exclude-module numpy ^
    --exclude-module pandas ^
    winghouse_app.py

if errorlevel 1 (
    echo.
    echo [ERROR] 빌드 실패. 위의 오류 메시지를 확인하세요.
    pause & exit /b 1
)

REM ── 6. .env 파일 복사 안내 ────────────────────────────────────────────────────
echo.
echo ============================================================
echo   빌드 성공!
echo ============================================================
echo.
echo   실행 파일 위치: dist\Winghouse_Crawler.exe
echo.
echo   [중요] 아래 파일을 exe 옆에 반드시 복사하세요:
echo          dist\.env        ← Supabase 접속 정보
echo.
if exist ".env" (
    copy ".env" "dist\.env" >nul
    echo   [자동] .env 파일을 dist\ 폴더에 자동 복사했습니다.
) else (
    echo   [경고] 루트에 .env 파일이 없습니다. 수동으로 복사하세요.
)
echo.
echo   [중요] 다른 PC에 배포할 경우:
echo          해당 PC에서 아래 명령 한 번 실행 필요:
echo          venv\Scripts\playwright.exe install chromium
echo          (또는 시스템 Python: playwright install chromium)
echo.

REM ── 7. build 폴더 및 .spec 정리 여부 확인 ────────────────────────────────────
set /p CLEAN="빌드 임시 파일(build\ 폴더, .spec)을 삭제할까요? [Y/N]: "
if /i "!CLEAN!"=="Y" (
    if exist "build" rmdir /s /q "build"
    if exist "Winghouse_Crawler.spec" (
        echo [INFO] Winghouse_Crawler.spec은 재빌드 시 필요하므로 유지합니다.
        echo        삭제하려면 수동으로 삭제하세요.
    )
    echo [OK] build\ 폴더 삭제 완료
)

echo.
echo 완료. dist\Winghouse_Crawler.exe 를 더블클릭하여 실행하세요.
echo.
pause
