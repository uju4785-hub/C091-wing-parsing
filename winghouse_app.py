"""
winghouse_app.py  —  윙하우스 상품 파서 (PyQt6 GUI) v5

크롤링 2단계 전략:
  Phase 1 : 카테고리 전 페이지 목록 스캔 → 상품 기본정보 수집
  Phase 2 :
    - 신규 상품           → 상세 페이지 방문, JS 변수 우선 추출 후 전체 저장
    - 가격 변경           → 상세 재방문, 옵션 전체 재저장
                            (비교 기준: parsing_wing_products.retail_price vs API 가격)
    - 품절 상태만 변경    → products.prod_cond + 모든 옵션 option_cond 빠른 업데이트
    - 변경 없음           → 완전 스킵

변경 사항 (v5):
  - DB 조회 단순화: 옵션 테이블 별도 조회 제거, 상품 테이블 retail_price 직접 비교
  - update_soldout_status: 옵션 일괄 업데이트 포함 (winghouse_parser 연동)
  - 통계 카운터(신규/업데이트/스킵/오류) 로직 유지

실행:
    python winghouse_app.py
"""

import sys
import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page
from supabase import create_client, Client

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout,
    QPushButton, QTextEdit, QPlainTextEdit, QProgressBar,
    QLabel, QGroupBox, QLineEdit,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QObject
from PyQt6.QtGui import QFont, QTextCursor, QColor, QPalette

# winghouse_parser 모듈에서 공통 상수·클래스·함수를 임포트
from winghouse_parser import (
    CATEGORY_URL,
    CATEGORY_ID,
    DETAIL_CONCURRENCY,   # 3 고정 (WinError 10035 방지)
    DB_BATCH_SIZE,        # 10 고정 (배치 저장 크기)
    ListingItem,
    ProductData,
    OptionData,
    _as_dict,
    _as_list,
    create_browser_context,
    fetch_all_listings_via_api,
    scrape_product_detail,
    scrape_with_new_page,
    save_product_and_options,
    save_product_async,
    save_products_batch,
    update_soldout_status,
    extract_wing_code,
    friendly_db_error,
)

load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
# Supabase (GUI 앱의 DB 조회용)
# ──────────────────────────────────────────────────────────────────────────────

SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ──────────────────────────────────────────────────────────────────────────────
# 워커 시그널 (메인 스레드 ↔ 크롤러 스레드 통신)
# ──────────────────────────────────────────────────────────────────────────────

class WorkerSignals(QObject):
    log_msg          = pyqtSignal(str, str)          # (message, level)
    page_progress    = pyqtSignal(int, int)          # (current_page, total_pages)
    product_progress = pyqtSignal(int, int)          # (current_idx, total_products)
    status           = pyqtSignal(str)               # 상태 텍스트
    counters         = pyqtSignal(int, int, int, int)  # new / upd / skip / fail
    finished         = pyqtSignal(dict)              # 최종 요약


# ──────────────────────────────────────────────────────────────────────────────
# 크롤러 워커 (QThread)
# ──────────────────────────────────────────────────────────────────────────────

class CrawlerWorker(QThread):

    def __init__(self) -> None:
        super().__init__()
        self.signals         = WorkerSignals()
        self._stop_requested = False

    def request_stop(self) -> None:
        self._stop_requested = True

    # ── QThread 진입점 ───────────────────────────────────────────────────────
    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._run_async())
        except Exception as exc:
            self.signals.log_msg.emit(f"치명적 오류: {exc}", "error")
        finally:
            loop.close()

    # ── 메인 비동기 루프 ───────────────────────────────────────────────────
    async def _run_async(self) -> None:
        started = datetime.now(timezone.utc)
        cnt_new = cnt_upd = cnt_skip = cnt_fail = 0

        self.signals.status.emit("브라우저 초기화 중...")

        async with async_playwright() as pw:
            ctx = await create_browser_context(pw)

            # ── Phase 1: API 루프로 전체 목록 수집 ────────────────────────
            self.signals.log_msg.emit("▶ Phase 1 시작 — API 루프 목록 수집", "phase")

            def _on_api_page(page: int, total_so_far: int) -> None:
                if self._stop_requested:
                    return
                self.signals.page_progress.emit(page, -1)
                self.signals.log_msg.emit(
                    f"  API page={page} — 누계 {total_so_far:,}건", "info"
                )

            all_listings: list[ListingItem] = await fetch_all_listings_via_api(
                ctx,
                on_page=_on_api_page,
            )

            total_products = len(all_listings)
            # 인디케이터 모드 해제: 수집 완료된 페이지 수로 확정 표시
            self.signals.page_progress.emit(total_products, total_products)
            self.signals.log_msg.emit(
                f"▶ Phase 1 완료 — 총 {total_products:,}개 상품 수집", "phase"
            )

            if total_products == 0 or self._stop_requested:
                await ctx.close()
                self.signals.finished.emit(
                    {"new": 0, "updated": 0, "skip": 0, "fail": 0, "elapsed": 0.0}
                )
                return

            # ── DB 기존 데이터 일괄 조회 ──────────────────────────────────
            self.signals.status.emit("DB 기존 데이터 조회 중...")
            self.signals.log_msg.emit("  DB 기존 데이터 조회 중...", "info")

            try:
                # parsing_wing_products: wing_code, prod_cond, retail_price
                # (옵션 테이블 별도 조회 불필요 — retail_price를 상품 테이블에서 직접 비교)
                prod_resp = (
                    supabase.table("parsing_wing_products")
                    .select("wing_code, prod_cond, retail_price")
                    .execute()
                )
            except Exception as exc:
                self.signals.log_msg.emit(
                    f"  DB 조회 실패: {friendly_db_error(exc)}", "error"
                )
                await ctx.close()
                self.signals.finished.emit(
                    {"new": 0, "updated": 0, "skip": 0, "fail": 0, "elapsed": 0.0}
                )
                return

            # wing_code → {prod_cond, retail_price} 맵
            existing_map: dict[str, dict] = {}
            for r in _as_list(prod_resp.data):
                wc_key = r.get("wing_code")
                if wc_key:
                    existing_map[wc_key] = {
                        "prod_cond":    int(r.get("prod_cond",    1) or 1),
                        "retail_price": int(r.get("retail_price", 0) or 0),
                    }
            self.signals.log_msg.emit(
                f"  DB 기존 상품: {len(existing_map):,}건", "info"
            )

            # ── Phase 2: 상품별 분류 ──────────────────────────────────────
            self.signals.log_msg.emit("▶ Phase 2 시작 — 상품 분류 및 처리", "phase")
            self.signals.product_progress.emit(0, total_products)

            # 처리 유형별 분류
            detail_items: list[tuple[str, ListingItem]] = []  # ("new"|"updated", item)
            cond_items:   list[tuple[ListingItem, int]]  = []  # (item, new_cond)
            skip_items:   list[ListingItem]              = []

            for item in all_listings:
                wc       = item.wing_code
                existing = existing_map.get(wc)
                if existing is None:
                    detail_items.append(("new", item))
                else:
                    old_retail = existing.get("retail_price") or 0
                    old_cond   = existing.get("prod_cond")    or 1
                    new_cond   = 2 if item.soldout else 1
                    price_diff = item.retail_price > 0 and old_retail != item.retail_price
                    cond_diff  = old_cond != new_cond
                    if price_diff:
                        detail_items.append(("updated", item))
                    elif cond_diff:
                        cond_items.append((item, new_cond))
                    else:
                        skip_items.append(item)

            self.signals.log_msg.emit(
                f"  분류 완료 — 신규/가격변경: {len(detail_items)}건, "
                f"품절변경: {len(cond_items)}건, 변경없음: {len(skip_items)}건",
                "info",
            )

            # ── 변경 없음 ─────────────────────────────────────────────────
            cnt_skip += len(skip_items)
            for item in skip_items:
                self.signals.log_msg.emit(
                    f"  [---] [{item.wing_code}] {item.name}  변경없음", "skip"
                )

            # ── 품절 상태만 변경 (DB 직접 업데이트, 상세 방문 불필요) ────
            _loop = asyncio.get_running_loop()
            for item, new_cond in cond_items:
                if self._stop_requested:
                    break
                wc = item.wing_code
                try:
                    await _loop.run_in_executor(
                        None, update_soldout_status, wc, item.soldout
                    )
                    cnt_upd += 1
                    label = "품절" if new_cond == 2 else "재판매"
                    self.signals.log_msg.emit(
                        f"  [UPD] [{wc}] {item.name}  ← {label}", "updated"
                    )
                except Exception as exc:
                    cnt_fail += 1
                    self.signals.log_msg.emit(
                        f"  [ERR] [{wc}] 품절 업데이트 실패\n"
                        f"        └ {friendly_db_error(exc)}",
                        "error",
                    )

            # ── 신규/가격변경: Semaphore 기반 배치 병렬 처리 ─────────────
            if detail_items and not self._stop_requested:
                self.signals.log_msg.emit(
                    f"  상세 방문 필요 {len(detail_items)}건 → "
                    f"동시 {DETAIL_CONCURRENCY}개 스크래핑, "
                    f"{DB_BATCH_SIZE}개 단위 DB 저장",
                    "info",
                )
                semaphore         = asyncio.Semaphore(DETAIL_CONCURRENCY)
                scrape_batch_size = DETAIL_CONCURRENCY * 2
                processed_detail  = 0
                _loop             = asyncio.get_running_loop()

                # 스크래핑 완료 결과를 누적하다가 DB_BATCH_SIZE마다 배치 저장
                pending_save: list[tuple[str, ListingItem, ProductData, list[OptionData]]] = []

                def _emit_log(msg: str) -> None:
                    self.signals.log_msg.emit(msg, "warn")

                async def _flush_pending() -> None:
                    nonlocal cnt_new, cnt_upd, cnt_fail
                    if not pending_save:
                        return
                    snapshot = list(pending_save)
                    pending_save.clear()
                    try:
                        saved_counts = await _loop.run_in_executor(
                            None,
                            lambda rows=snapshot: save_products_batch(
                                [(p, o) for _, _, p, o in rows],
                                _emit_log,
                            ),
                        )
                        for (kind, orig_item, product, options), saved in zip(snapshot, saved_counts):
                            _min_p = min(
                                (product.retail_price + o.add_price
                                 for o in options
                                 if product.retail_price + o.add_price > 0),
                                default=product.retail_price,
                            )
                            _st = "품절" if product.soldout else "판매중"
                            if kind == "new":
                                cnt_new  += 1
                                log_level = "new"
                            else:
                                cnt_upd  += 1
                                log_level = "updated"
                            self.signals.log_msg.emit(
                                f"  [수집완료] {product.name} "
                                f"(기본가: {product.retail_price:,}원, "
                                f"최저가: {_min_p:,}원, 상태: {_st}, 옵션: {saved}건)",
                                log_level,
                            )
                    except Exception as exc:
                        cnt_fail += len(snapshot)
                        self.signals.log_msg.emit(
                            f"  [ERR] 배치 DB 저장 실패 ({len(snapshot)}건)\n"
                            f"        └ {friendly_db_error(exc)}",
                            "error",
                        )

                for batch_start in range(0, len(detail_items), scrape_batch_size):
                    if self._stop_requested:
                        self.signals.log_msg.emit("⏹ 사용자 요청으로 중단", "warn")
                        break

                    batch = detail_items[batch_start : batch_start + scrape_batch_size]

                    # ── 배치 병렬 스크래핑 ──────────────────────────────
                    scrape_results = await asyncio.gather(
                        *[scrape_with_new_page(ctx, item, semaphore)
                          for _, item in batch],
                        return_exceptions=True,
                    )

                    # ── 스크래핑 결과 분류 ──────────────────────────────
                    for (kind, orig_item), result in zip(batch, scrape_results):
                        wc = orig_item.wing_code
                        if isinstance(result, Exception):
                            cnt_fail += 1
                            self.signals.log_msg.emit(
                                f"  [ERR] [{wc}] {orig_item.name}\n"
                                f"        └ {friendly_db_error(result)}",
                                "error",
                            )
                            continue
                        _, product, options, err = result
                        if err is not None:
                            cnt_fail += 1
                            self.signals.log_msg.emit(
                                f"  [ERR] [{wc}] {orig_item.name}\n"
                                f"        └ {friendly_db_error(err)}",
                                "error",
                            )
                            continue
                        pending_save.append((kind, orig_item, product, options))

                    # DB_BATCH_SIZE 충족 시 배치 저장 실행
                    if len(pending_save) >= DB_BATCH_SIZE:
                        await _flush_pending()

                    processed_detail += len(batch)
                    current_total = processed_detail + len(cond_items) + len(skip_items)
                    self.signals.product_progress.emit(current_total, total_products)
                    self.signals.counters.emit(cnt_new, cnt_upd, cnt_skip, cnt_fail)
                    self.signals.status.emit(
                        f"처리 중 [{current_total}/{total_products}]  스크래핑 배치 완료"
                    )

                # 잔여분 저장
                await _flush_pending()
                self.signals.counters.emit(cnt_new, cnt_upd, cnt_skip, cnt_fail)

            else:
                # detail_items 없을 때도 카운터/진행 갱신
                self.signals.product_progress.emit(total_products, total_products)
                self.signals.counters.emit(cnt_new, cnt_upd, cnt_skip, cnt_fail)

            await ctx.close()

        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        self.signals.finished.emit({
            "new":     cnt_new,
            "updated": cnt_upd,
            "skip":    cnt_skip,
            "fail":    cnt_fail,
            "elapsed": elapsed,
        })


# ──────────────────────────────────────────────────────────────────────────────
# 단일 상품 워커 (QThread)
# ──────────────────────────────────────────────────────────────────────────────

class SingleProductWorker(QThread):
    """텍스트박스에 입력된 URL 목록을 순서대로 크롤링하여 저장합니다."""

    def __init__(self, urls: list[str]) -> None:
        super().__init__()
        self.urls    = urls
        self.signals = WorkerSignals()

    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._run_async())
        except Exception as exc:
            self.signals.log_msg.emit(f"치명적 오류: {exc}", "error")
        finally:
            loop.close()

    async def _run_async(self) -> None:
        started   = datetime.now(timezone.utc)
        cnt_new   = cnt_upd = cnt_fail = 0
        total     = len(self.urls)

        self.signals.status.emit("브라우저 초기화 중...")
        self.signals.product_progress.emit(0, total)

        async with async_playwright() as pw:
            ctx  = await create_browser_context(pw)
            page = await ctx.new_page()

            for idx, url in enumerate(self.urls, 1):
                url = url.strip()
                if not url:
                    continue

                wing_code = extract_wing_code(url)
                if not wing_code:
                    self.signals.log_msg.emit(
                        f"  [ERR] URL에서 상품 코드를 추출할 수 없습니다: {url}", "error"
                    )
                    cnt_fail += 1
                    self.signals.product_progress.emit(idx, total)
                    self.signals.counters.emit(cnt_new, cnt_upd, 0, cnt_fail)
                    continue

                self.signals.status.emit(f"처리 중 [{idx}/{total}]  {url}")
                self.signals.log_msg.emit(
                    f"▶ [{idx}/{total}] 크롤링 시작 — wing_code={wing_code}", "phase"
                )

                item = ListingItem(
                    url          = url,
                    wing_code    = wing_code,
                    name         = "",
                    retail_price = 0,
                    model        = "",
                    image_url    = "",
                    soldout      = False,
                )

                last_err: Optional[Exception] = None
                for attempt in range(1, 3):
                    try:
                        product, options = await scrape_product_detail(page, item)
                        saved = save_product_and_options(
                            product, options,
                            on_status_change=lambda msg: self.signals.log_msg.emit(msg, "warn"),
                        )
                        _min_p = min(
                            (product.retail_price + o.add_price
                             for o in options
                             if product.retail_price + o.add_price > 0),
                            default=product.retail_price,
                        )
                        _st = "품절" if product.soldout else "판매중"
                        self.signals.log_msg.emit(
                            f"  [완료] {product.name} "
                            f"(기본가: {product.retail_price:,}원, "
                            f"최저가: {_min_p:,}원, 상태: {_st}, 옵션: {saved}건)",
                            "new",
                        )
                        cnt_new += 1
                        last_err = None
                        break
                    except Exception as exc:
                        last_err = exc
                        if attempt < 2:
                            await asyncio.sleep(2)

                if last_err:
                    cnt_fail += 1
                    self.signals.log_msg.emit(
                        f"  [ERR] [{wing_code}] 저장 실패\n"
                        f"        └ {friendly_db_error(last_err)}",
                        "error",
                    )

                self.signals.product_progress.emit(idx, total)
                self.signals.counters.emit(cnt_new, cnt_upd, 0, cnt_fail)

            await ctx.close()

        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        self.signals.finished.emit({
            "new":     cnt_new,
            "updated": cnt_upd,
            "skip":    0,
            "fail":    cnt_fail,
            "elapsed": elapsed,
        })


# ──────────────────────────────────────────────────────────────────────────────
# 로그 위젯 (컬러 출력)
# ──────────────────────────────────────────────────────────────────────────────

_LOG_COLORS = {
    "new":     "#00CC66",
    "updated": "#FF9900",
    "skip":    "#555555",
    "error":   "#FF4444",
    "warn":    "#FFCC00",
    "phase":   "#4499FF",
    "info":    "#888888",
}


class LogWidget(QTextEdit):
    def __init__(self) -> None:
        super().__init__()
        self.setReadOnly(True)
        self.setFont(QFont("Consolas", 9))
        self.setStyleSheet("QTextEdit { background:#1a1a1a; border:none; }")

    def append_log(self, message: str, level: str = "info") -> None:
        color = _LOG_COLORS.get(level, "#888888")
        ts    = datetime.now().strftime("%H:%M:%S")
        safe  = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        # 여러 줄 지원: \n → <br>
        safe  = safe.replace("\n", "<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")
        html  = (
            f'<span style="color:#444;">{ts}</span>&nbsp;'
            f'<span style="color:{color};">{safe}</span><br>'
        )
        self.moveCursor(QTextCursor.MoveOperation.End)
        self.insertHtml(html)
        self.moveCursor(QTextCursor.MoveOperation.End)


# ──────────────────────────────────────────────────────────────────────────────
# 메인 윈도우
# ──────────────────────────────────────────────────────────────────────────────

class MainWindow(QMainWindow):

    def __init__(self) -> None:
        super().__init__()
        self._worker:        Optional[CrawlerWorker]       = None
        self._single_worker: Optional[SingleProductWorker] = None
        self._setup_ui()

    def _setup_ui(self) -> None:
        self.setWindowTitle("윙하우스 상품 파서")
        self.setMinimumSize(960, 720)

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setSpacing(8)
        root.setContentsMargins(12, 12, 12, 12)

        # ── 헤더 ──
        header = QLabel("WINGHOUSE  상품 파서")
        header.setFont(QFont("맑은 고딕", 13, QFont.Weight.Bold))
        header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        header.setStyleSheet(
            "color:#E8E8E8; background:#0F3460; padding:10px; border-radius:6px;"
        )
        root.addWidget(header)

        # ── 컨트롤 행 ──
        ctrl = QHBoxLayout()
        self.btn_start  = QPushButton("▶   전체 업데이트 시작")
        self.btn_stop   = QPushButton("⏹   중단")
        self.lbl_status = QLabel("대기 중")

        for btn, bg, hover in [
            (self.btn_start, "#16213E", "#0F3460"),
            (self.btn_stop,  "#7B1E1E", "#9B2020"),
        ]:
            btn.setFixedHeight(38)
            btn.setStyleSheet(
                f"QPushButton{{background:{bg};color:#E8E8E8;font-weight:bold;"
                f"border:1px solid #333;border-radius:4px;padding:0 20px;}}"
                f"QPushButton:hover{{background:{hover};}}"
                f"QPushButton:disabled{{background:#2A2A2A;color:#555;}}"
            )

        self.btn_stop.setEnabled(False)
        self.lbl_status.setStyleSheet("color:#AAAAAA; padding-left:12px;")

        ctrl.addWidget(self.btn_start)
        ctrl.addWidget(self.btn_stop)
        ctrl.addWidget(self.lbl_status)
        ctrl.addStretch()
        root.addLayout(ctrl)

        # ── 단일 상품 URL 입력 그룹 ──
        url_group = QGroupBox("단일 상품 크롤링 (URL 입력)")
        url_group.setStyleSheet(self._group_style())
        url_layout = QVBoxLayout(url_group)
        url_layout.setSpacing(6)

        url_hint = QLabel("상품 URL을 한 줄에 하나씩 입력하세요 (여러 개 가능)")
        url_hint.setStyleSheet("color:#777777; font-size:10px;")
        url_layout.addWidget(url_hint)

        url_input_row = QHBoxLayout()
        self.txt_urls = QPlainTextEdit()
        self.txt_urls.setPlaceholderText(
            "https://winghouse.co.kr/product/상품명/1234/\n"
            "https://winghouse.co.kr/product/상품명/5678/"
        )
        self.txt_urls.setFixedHeight(72)
        self.txt_urls.setFont(QFont("Consolas", 9))
        self.txt_urls.setStyleSheet(
            "QPlainTextEdit { background:#111827; color:#CCCCCC; "
            "border:1px solid #3A3A3A; border-radius:4px; padding:4px; }"
        )

        self.btn_single = QPushButton("🔍  크롤링 시작")
        self.btn_single.setFixedSize(130, 72)
        self.btn_single.setStyleSheet(
            "QPushButton{background:#1A3A2A;color:#E8E8E8;font-weight:bold;"
            "border:1px solid #2A5A3A;border-radius:4px;padding:0 12px;}"
            "QPushButton:hover{background:#1E5A34;}"
            "QPushButton:disabled{background:#2A2A2A;color:#555;}"
        )

        url_input_row.addWidget(self.txt_urls)
        url_input_row.addWidget(self.btn_single)
        url_layout.addLayout(url_input_row)
        root.addWidget(url_group)

        # ── 진행 그룹 ──
        pg_group = QGroupBox("진행 현황")
        pg_group.setStyleSheet(self._group_style())
        pg_layout = QVBoxLayout(pg_group)
        pg_layout.setSpacing(6)

        row1 = QHBoxLayout()
        self.lbl_page = QLabel("페이지 스캔     0 / ?")
        self.lbl_page.setFixedWidth(200)
        self.lbl_page.setStyleSheet("color:#AAAAAA;")
        self.bar_page = QProgressBar()
        self.bar_page.setTextVisible(True)
        self.bar_page.setStyleSheet(self._bar_style("#4499FF"))
        row1.addWidget(self.lbl_page)
        row1.addWidget(self.bar_page)
        pg_layout.addLayout(row1)

        row2 = QHBoxLayout()
        self.lbl_prod = QLabel("상품 처리       0 / 0")
        self.lbl_prod.setFixedWidth(200)
        self.lbl_prod.setStyleSheet("color:#AAAAAA;")
        self.bar_prod = QProgressBar()
        self.bar_prod.setTextVisible(True)
        self.bar_prod.setStyleSheet(self._bar_style("#00CC66"))
        row2.addWidget(self.lbl_prod)
        row2.addWidget(self.bar_prod)
        pg_layout.addLayout(row2)

        cnt_row = QHBoxLayout()
        self.lbl_new  = self._make_counter("신규",    "#00CC66")
        self.lbl_upd  = self._make_counter("업데이트", "#FF9900")
        self.lbl_skip = self._make_counter("변경없음", "#555555")
        self.lbl_fail = self._make_counter("오류",    "#FF4444")
        for lbl in (self.lbl_new, self.lbl_upd, self.lbl_skip, self.lbl_fail):
            cnt_row.addWidget(lbl)
        cnt_row.addStretch()
        pg_layout.addLayout(cnt_row)
        root.addWidget(pg_group)

        # ── 로그 그룹 ──
        log_group = QGroupBox("실시간 로그")
        log_group.setStyleSheet(self._group_style())
        log_layout = QVBoxLayout(log_group)
        log_layout.setContentsMargins(4, 4, 4, 4)
        self.log_widget = LogWidget()
        log_layout.addWidget(self.log_widget)
        root.addWidget(log_group, stretch=1)

        self.setStyleSheet(
            "QMainWindow, QWidget { background:#1E1E2E; color:#CCCCCC; }"
            "QGroupBox { margin-top:4px; }"
        )

        self.btn_start.clicked.connect(self._on_start)
        self.btn_stop.clicked.connect(self._on_stop)
        self.btn_single.clicked.connect(self._on_single_start)

    @staticmethod
    def _group_style() -> str:
        return (
            "QGroupBox { color:#BBBBBB; border:1px solid #333; "
            "border-radius:5px; padding-top:10px; }"
            "QGroupBox::title { padding:0 6px; }"
        )

    @staticmethod
    def _bar_style(color: str) -> str:
        return (
            f"QProgressBar {{ border:1px solid #3A3A3A; border-radius:3px; "
            f"background:#111; text-align:center; color:#CCC; }}"
            f"QProgressBar::chunk {{ background:{color}; border-radius:3px; }}"
        )

    @staticmethod
    def _make_counter(title: str, color: str) -> QLabel:
        lbl = QLabel()
        lbl.setTextFormat(Qt.TextFormat.RichText)
        lbl.setStyleSheet("padding:0 10px;")
        MainWindow._set_counter(lbl, title, color, 0)
        return lbl

    @staticmethod
    def _set_counter(lbl: QLabel, title: str, color: str, n: int) -> None:
        lbl.setText(
            f'<span style="color:{color};">● {title}: </span>'
            f'<b style="color:#FFFFFF;">{n:,}건</b>'
        )

    # ── 슬롯 ────────────────────────────────────────────────────────────────
    def _on_start(self) -> None:
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.bar_page.setMaximum(1)
        self.bar_page.setValue(0)
        self.bar_prod.setMaximum(1)
        self.bar_prod.setValue(0)
        self.lbl_page.setText("페이지 스캔     0 / ?")
        self.lbl_prod.setText("상품 처리       0 / 0")
        for lbl, t, c in [
            (self.lbl_new,  "신규",    "#00CC66"),
            (self.lbl_upd,  "업데이트", "#FF9900"),
            (self.lbl_skip, "변경없음", "#555555"),
            (self.lbl_fail, "오류",    "#FF4444"),
        ]:
            self._set_counter(lbl, t, c, 0)

        self.log_widget.clear()
        self.log_widget.append_log("═══ 크롤링 시작 ═══", "phase")

        self._worker = CrawlerWorker()
        sig = self._worker.signals
        sig.log_msg.connect(self.log_widget.append_log)
        sig.page_progress.connect(self._on_page_progress)
        sig.product_progress.connect(self._on_product_progress)
        sig.status.connect(self.lbl_status.setText)
        sig.counters.connect(self._on_counters)
        sig.finished.connect(self._on_finished)
        self._worker.finished.connect(self._worker.deleteLater)
        self._worker.start()

    def _on_stop(self) -> None:
        if self._worker:
            self._worker.request_stop()
            self.btn_stop.setEnabled(False)
            self.lbl_status.setText("중단 요청 중...")

    def _on_single_start(self) -> None:
        raw_text = self.txt_urls.toPlainText().strip()
        if not raw_text:
            self.log_widget.append_log("URL을 입력해주세요.", "warn")
            return

        urls = [u.strip() for u in raw_text.splitlines() if u.strip()]
        if not urls:
            self.log_widget.append_log("유효한 URL이 없습니다.", "warn")
            return

        self.btn_single.setEnabled(False)
        self.btn_start.setEnabled(False)
        self.bar_prod.setMaximum(len(urls))
        self.bar_prod.setValue(0)
        self.lbl_prod.setText(f"상품 처리       0 / {len(urls)}")
        for lbl, t, c in [
            (self.lbl_new,  "신규",    "#00CC66"),
            (self.lbl_upd,  "업데이트", "#FF9900"),
            (self.lbl_skip, "변경없음", "#555555"),
            (self.lbl_fail, "오류",    "#FF4444"),
        ]:
            self._set_counter(lbl, t, c, 0)

        self.log_widget.append_log(
            f"═══ 단일 크롤링 시작 — {len(urls)}개 URL ═══", "phase"
        )

        self._single_worker = SingleProductWorker(urls)
        sig = self._single_worker.signals
        sig.log_msg.connect(self.log_widget.append_log)
        sig.product_progress.connect(self._on_product_progress)
        sig.status.connect(self.lbl_status.setText)
        sig.counters.connect(self._on_counters)
        sig.finished.connect(self._on_single_finished)
        self._single_worker.finished.connect(self._single_worker.deleteLater)
        self._single_worker.start()

    def _on_single_finished(self, stats: dict) -> None:
        self.btn_single.setEnabled(True)
        self.btn_start.setEnabled(True)
        elapsed = stats.get("elapsed", 0.0)
        m, s = divmod(int(elapsed), 60)
        self.lbl_status.setText(f"단일 크롤링 완료  ({m}분 {s}초 소요)")
        self.log_widget.append_log(
            f"═══ 단일 크롤링 완료  신규/업데이트 {stats['new']:,}건 · "
            f"오류 {stats['fail']:,}건  ({m}분 {s}초) ═══",
            "phase",
        )

    def _on_page_progress(self, cur: int, total: int) -> None:
        if total < 0:
            # API 방식: 총 페이지 수 미확정 → 프로그레스바를 인디케이터로 표시
            self.bar_page.setMaximum(0)
            self.lbl_page.setText(f"페이지 스캔     {cur:,} 페이지 완료 (진행 중)")
        else:
            self.bar_page.setMaximum(max(total, 1))
            self.bar_page.setValue(cur)
            self.lbl_page.setText(f"페이지 스캔     {cur:,} / {total:,}")

    def _on_product_progress(self, cur: int, total: int) -> None:
        self.bar_prod.setMaximum(max(total, 1))
        self.bar_prod.setValue(cur)
        self.lbl_prod.setText(f"상품 처리       {cur:,} / {total:,}")

    def _on_counters(self, new: int, upd: int, skip: int, fail: int) -> None:
        self._set_counter(self.lbl_new,  "신규",    "#00CC66", new)
        self._set_counter(self.lbl_upd,  "업데이트", "#FF9900", upd)
        self._set_counter(self.lbl_skip, "변경없음", "#555555", skip)
        self._set_counter(self.lbl_fail, "오류",    "#FF4444", fail)

    def _on_finished(self, stats: dict) -> None:
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        elapsed = stats.get("elapsed", 0.0)
        m, s = divmod(int(elapsed), 60)
        self.lbl_status.setText(f"완료  ({m}분 {s}초 소요)")
        self.log_widget.append_log(
            f"═══ 완료  신규 {stats['new']:,}건 · 업데이트 {stats['updated']:,}건 · "
            f"변경없음 {stats['skip']:,}건 · 오류 {stats['fail']:,}건  "
            f"({m}분 {s}초) ═══",
            "phase",
        )


# ──────────────────────────────────────────────────────────────────────────────
# 진입점
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    palette = QPalette()
    palette.setColor(QPalette.ColorRole.Window,      QColor("#1E1E2E"))
    palette.setColor(QPalette.ColorRole.WindowText,  QColor("#CCCCCC"))
    palette.setColor(QPalette.ColorRole.Base,        QColor("#1A1A2E"))
    palette.setColor(QPalette.ColorRole.Text,        QColor("#CCCCCC"))
    palette.setColor(QPalette.ColorRole.Button,      QColor("#16213E"))
    palette.setColor(QPalette.ColorRole.ButtonText,  QColor("#E8E8E8"))
    app.setPalette(palette)

    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
