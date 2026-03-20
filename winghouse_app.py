"""
winghouse_app.py  —  윙하우스 상품 파서 (PyQt6 GUI)

크롤링 2단계 전략:
  Phase 1 : 카테고리 전 페이지 목록 스캔 → 상품 기본정보 수집
  Phase 2 :
    - 신규 상품           → 상세 페이지 방문, JS 변수 우선 추출 후 전체 저장
    - 가격 변경           → 상세 재방문, 옵션 전체 재저장
    - 품절 상태만 변경    → products.prod_cond 빠른 업데이트 (상세 방문 없음)
    - 변경 없음           → 완전 스킵

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
    QPushButton, QTextEdit, QProgressBar,
    QLabel, QGroupBox,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QObject
from PyQt6.QtGui import QFont, QTextCursor, QColor, QPalette

# winghouse_parser 모듈에서 공통 상수·클래스·함수를 임포트
from winghouse_parser import (
    CATEGORY_URL,
    CATEGORY_ID,
    ListingItem,
    ProductData,
    OptionData,
    _as_dict,
    _as_list,
    create_browser_context,
    fetch_all_listings_via_api,
    scrape_product_detail,
    save_product_and_options,
    update_soldout_status,
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
            ctx      = await create_browser_context(pw)
            det_page = await ctx.new_page()

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
                # parsing_wing_products: id, wing_code, prod_cond
                prod_resp = (
                    supabase.table("parsing_wing_products")
                    .select("id, wing_code, prod_cond")
                    .execute()
                )
                # parsing_wing_options: product_id별 최솟값 retail_price
                opt_resp = (
                    supabase.table("parsing_wing_options")
                    .select("product_id, retail_price")
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

            # product_id → 최솟값 retail_price 맵
            # _as_list() 로 supabase-py 버전별 str/dict 차이를 흡수
            opt_price_map: dict[int, int] = {}
            for row in _as_list(opt_resp.data):
                pid   = row.get("product_id")
                price = int(row.get("retail_price") or 0)
                if pid is not None and (
                    pid not in opt_price_map or price < opt_price_map[pid]
                ):
                    opt_price_map[pid] = price

            # wing_code → {id, prod_cond, opt_price} 통합 맵
            existing_map: dict[str, dict] = {}
            for r in _as_list(prod_resp.data):
                wc_key = r.get("wing_code")
                rid    = r.get("id")
                if wc_key:
                    existing_map[wc_key] = {
                        "id":        rid,
                        "prod_cond": r.get("prod_cond", 1),
                        "opt_price": opt_price_map.get(rid, 0),
                    }
            self.signals.log_msg.emit(
                f"  DB 기존 상품: {len(existing_map):,}건", "info"
            )

            # ── Phase 2: 상품별 처리 ──────────────────────────────────────
            self.signals.log_msg.emit("▶ Phase 2 시작 — 상품 처리", "phase")
            self.signals.product_progress.emit(0, total_products)

            for idx, item in enumerate(all_listings, 1):
                if self._stop_requested:
                    self.signals.log_msg.emit("⏹ 사용자 요청으로 중단", "warn")
                    break

                wc = item.wing_code
                self.signals.product_progress.emit(idx, total_products)
                self.signals.status.emit(
                    f"처리 중 [{idx}/{total_products}]  {item.name[:25]}"
                )

                existing = existing_map.get(wc)

                if existing is None:
                    # ── 신규: 상세 페이지 방문 → JS 변수 추출 → 전체 저장 ──
                    last_err: Optional[Exception] = None
                    for attempt in range(1, 3):
                        try:
                            product, options = await scrape_product_detail(det_page, item)
                            saved = save_product_and_options(
                                product, options,
                                on_status_change=lambda msg: self.signals.log_msg.emit(msg, "warn"),
                            )
                            cnt_new += 1
                            _min_p = min(
                                (o.retail_price for o in options if o.retail_price > 0),
                                default=0,
                            )
                            _st = "품절" if product.soldout else "판매중"
                            self.signals.log_msg.emit(
                                f"  [수집완료] {product.name} "
                                f"(가격: {_min_p:,}원, 상태: {_st}, 옵션: {saved}건)",
                                "new",
                            )
                            last_err = None
                            break
                        except Exception as exc:
                            last_err = exc
                            if attempt < 2:
                                await asyncio.sleep(2)
                    if last_err:
                        cnt_fail += 1
                        self.signals.log_msg.emit(
                            f"  [ERR] [{wc}] {item.name}\n"
                            f"        └ {friendly_db_error(last_err)}",
                            "error",
                        )

                else:
                    # ── 기존: 가격·품절 변경 비교 ─────────────────────────
                    old_opt_price = existing.get("opt_price") or 0
                    old_cond      = existing.get("prod_cond") or 1
                    new_cond      = 2 if item.soldout else 1
                    price_diff    = item.retail_price > 0 and old_opt_price != item.retail_price
                    cond_diff     = old_cond != new_cond

                    if price_diff:
                        # 가격 변경 → 상세 재방문해서 옵션·재고 전체 재저장
                        last_err2: Optional[Exception] = None
                        for attempt in range(1, 3):
                            try:
                                product, options = await scrape_product_detail(det_page, item)
                                saved = save_product_and_options(
                                    product, options,
                                    on_status_change=lambda msg: self.signals.log_msg.emit(msg, "warn"),
                                )
                                cnt_upd += 1
                                _min_p2 = min(
                                    (o.retail_price for o in options if o.retail_price > 0),
                                    default=0,
                                )
                                _st2 = "품절" if product.soldout else "판매중"
                                self.signals.log_msg.emit(
                                    f"  [수집완료] {product.name} "
                                    f"(가격: {_min_p2:,}원, 상태: {_st2}, 옵션: {saved}건)",
                                    "updated",
                                )
                                last_err2 = None
                                break
                            except Exception as exc:
                                last_err2 = exc
                                if attempt < 2:
                                    await asyncio.sleep(2)
                        if last_err2:
                            cnt_fail += 1
                            self.signals.log_msg.emit(
                                f"  [ERR] [{wc}] 가격 업데이트 실패\n"
                                f"        └ {friendly_db_error(last_err2)}",
                                "error",
                            )

                    elif cond_diff:
                        # 품절 상태만 변경 → prod_cond 만 빠르게 업데이트
                        try:
                            update_soldout_status(wc, item.soldout)
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

                    else:
                        cnt_skip += 1
                        self.signals.log_msg.emit(
                            f"  [---] [{wc}] {item.name}  변경없음", "skip"
                        )

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
        self._worker: Optional[CrawlerWorker] = None
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
