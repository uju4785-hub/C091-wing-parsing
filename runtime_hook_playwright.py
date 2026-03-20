# runtime_hook_playwright.py
#
# PyInstaller 런타임 훅:
# exe 실행 시 Playwright가 브라우저를 올바르게 찾도록
# PLAYWRIGHT_BROWSERS_PATH 환경변수를 설정합니다.
#
# 탐색 순서:
#   1. exe 옆의 playwright_browsers\ 폴더 (휴대용 배포 시)
#   2. 사용자 기본 설치 위치 (%LOCALAPPDATA%\ms-playwright)
#   3. 환경변수가 이미 설정된 경우 그대로 사용
#
import os
import sys

def _setup_playwright_path() -> None:
    if os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        return  # 이미 설정됨

    # exe 실행 경로 (--onefile 시 sys.executable, 개발 시 sys.argv[0])
    exe_dir = os.path.dirname(
        sys.executable if getattr(sys, "frozen", False) else os.path.abspath(sys.argv[0])
    )

    # 옵션 1: exe 옆의 playwright_browsers 폴더 (휴대용 배포)
    portable = os.path.join(exe_dir, "playwright_browsers")
    if os.path.isdir(portable):
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = portable
        return

    # 옵션 2: Windows 기본 설치 위치
    local_app_data = os.environ.get("LOCALAPPDATA", "")
    default = os.path.join(local_app_data, "ms-playwright")
    if os.path.isdir(default):
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = default

_setup_playwright_path()
