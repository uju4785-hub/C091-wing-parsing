"""
winghouse_parser.py  —  윙하우스 상세 페이지 파싱 모듈 v5

변경 사항 (v5):
  - parsing_wing_products: opt3_title / retail_price / prev_retail_price /
    price_updated_at / soldout_at / resale_at 처리 추가, regi_cond 제거
  - parsing_wing_options: wing_code 기반 저장 (product_id FK 제거),
    option_code(item_code 기반) / opt3_name / add_price / stock_count /
    soldout_at / resale_at 처리 추가, cost_price/changed_at 제거
  - product_external_mappings: 신규 상품 등록 시 status='pending' 자동 생성
  - scrape_product_detail: ld+json offers 파싱 강화 → opt3_name / option_code 보조
  - save_product_and_options: 선조회 → 비교 → 조건부 upsert (가격·상태 변동 추적)
  - update_soldout_status: 상품 + 옵션 동시 일괄 업데이트

JS 추출 전략 (변경 없음):
  var product_name        → name
  ld+json image[0]        → image_url  (폴백: og:image)
  var option_name_mapper  → opt1_title / opt2_title / opt3_title  (#$% 구분자)
  var option_stock_data   → 옵션 목록 (item_code, opt1~3_name, option_price, stock_number)
  ld+json offers          → option_code(sku) / opt1~3_name 폴백
  #prdDetail innerHTML    → description

실행 예시:
  python winghouse_parser.py --pages 1
"""

import os
import re
import asyncio
import logging
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from supabase import create_client, Client

load_dotenv()

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Supabase
# ──────────────────────────────────────────────────────────────────────────────

SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ──────────────────────────────────────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────────────────────────────────────

CATEGORY_URL = "https://winghouse.co.kr/category/%EC%A0%84%EC%B2%B4%EB%B3%B4%EA%B8%B0/134/"
CATEGORY_ID  = 134

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36"
)

STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver',  { get: () => undefined });
Object.defineProperty(navigator, 'plugins',    { get: () => [1,2,3,4,5] });
Object.defineProperty(navigator, 'languages',  { get: () => ['ko-KR','ko','en-US','en'] });
Object.defineProperty(navigator, 'platform',   { get: () => 'Win32' });
window.chrome = { runtime:{}, loadTimes:function(){}, csi:function(){}, app:{} };
"""

# 차단할 리소스 유형 (텍스트/JSON 추출에 불필요한 것들)
BLOCK_RESOURCE_TYPES = frozenset({"image", "font", "stylesheet", "media"})

# ── DB 소켓 과부하 방지 설정 ────────────────────────────────────────────────
# WinError 10035: 비동기 소켓 버퍼 초과(Windows) — 동시 DB 연결 수 제한 필요
DETAIL_CONCURRENCY: int   = 3    # 상세 페이지 최대 동시 처리 수 (3~5 권장)
DB_BATCH_SIZE:      int   = 10   # 한 번에 DB에 저장할 상품 묶음 크기
DB_MAX_RETRIES:     int   = 3    # 소켓 오류 시 최대 재시도 횟수
DB_RETRY_DELAY:     float = 1.0  # 재시도 대기 시간(초)

# ──────────────────────────────────────────────────────────────────────────────
# JS 추출기 — 목록 페이지
# ──────────────────────────────────────────────────────────────────────────────

LISTING_JS = r"""
() => {
    const PROD_RE = /\/product\/[^\/]+\/(\d+)\//;
    const seen    = new Set();
    const result  = [];

    for (const link of document.querySelectorAll('a[href*="/product/"]')) {
        const url = link.href;
        const m   = url.match(PROD_RE);
        if (!m) continue;

        const wc = m[1];
        if (seen.has(wc)) continue;
        seen.add(wc);

        const card   = link.closest('li') || link.closest('.xans-record-') || link.parentElement;
        const nameEl = card && card.querySelector('.name a, strong.name a');
        const name   = (nameEl || link).innerText.replace(/^상품명\s*[:\s]+/, '').trim();
        if (!name || name.length > 200) continue;

        const imgEl     = card && card.querySelector('.thumbnail img, img');
        const image_url = (imgEl && imgEl.src) ? imgEl.src : '';

        const txt      = (card && card.innerText) || '';
        const priceM   = txt.match(/판매가\s*[:\s]+([\d,]+)/);
        const modelM   = txt.match(/자체상품코드\s*[:\s]+([^\n,]+)/);
        const retail_price = priceM ? parseInt(priceM[1].replace(/,/g, '')) || 0 : 0;
        const model        = modelM ? modelM[1].trim() : '';
        const soldout      = !!(
            (card && card.querySelector('[class*="soldout"],[class*="soldOut"]')) ||
            txt.includes('품절')
        );

        result.push({ url, wing_code: wc, name, retail_price, model, image_url, soldout });
    }
    return result;
}
"""

# ──────────────────────────────────────────────────────────────────────────────
# JS 추출기 — 상세 페이지 (JS 변수 우선)
# ──────────────────────────────────────────────────────────────────────────────

DETAIL_JS = r"""
() => {
    const get = (key) => {
        try { return window[key] !== undefined ? window[key] : null; }
        catch(e) { return null; }
    };

    // ── 기본 정보 ─────────────────────────────────────────────────────────
    const name = get('product_name') ||
        (document.querySelector('.headingArea h2, h2.name, #contents h2') || {}).innerText?.trim() || '';

    // ── 대표 이미지: ld+json image[0] 우선, og:image 폴백 ─────────────────
    let image_url = '';
    for (const script of document.querySelectorAll('script[type="application/ld+json"]')) {
        try {
            const data = JSON.parse(script.textContent || '');
            const imgs = data.image ?? (Array.isArray(data['@graph']) ? (data['@graph'][0] || {}).image : null) ?? [];
            const first = Array.isArray(imgs) ? imgs[0] : (typeof imgs === 'string' ? imgs : '');
            if (first) { image_url = first; break; }
        } catch(e) {}
    }
    if (!image_url) {
        const og = document.querySelector('meta[property="og:image"]');
        image_url = og ? (og.getAttribute('content') || '') : '';
    }
    if (image_url.startsWith('//')) image_url = 'https:' + image_url;

    const base_price = parseInt(
        String(get('product_price') || '0').replace(/[^0-9]/g, '')
    ) || 0;

    // ── 자체 상품코드 ──────────────────────────────────────────────────────
    let custom_code = String(get('product_custom_code') || get('product_code') || '');
    if (!custom_code) {
        for (const th of document.querySelectorAll('th')) {
            if ((th.textContent || '').includes('상품코드')) {
                const tr = th.closest('tr');
                const td = tr ? tr.querySelector('td') : th.nextElementSibling;
                const val = td ? td.textContent.trim() : '';
                if (val) { custom_code = val; break; }
            }
        }
    }

    // ── 전용여부 ───────────────────────────────────────────────────────────
    let exclusive_label = '';
    for (const th of document.querySelectorAll('th')) {
        if ((th.textContent || '').trim() === '전용여부') {
            const tr = th.closest('tr');
            const td = tr ? tr.querySelector('td') : th.nextElementSibling;
            exclusive_label = td ? td.textContent.trim() : '';
            break;
        }
    }

    // ── 옵션 타이틀: option_name_mapper (#$% 최대 3단) ───────────────────
    let opt1_title = null, opt2_title = null, opt3_title = null;
    const mapper = get('option_name_mapper');
    if (typeof mapper === 'string' && mapper.trim()) {
        const parts = mapper.split('#$%');
        opt1_title = parts[0] || null;
        opt2_title = parts[1] || null;
        opt3_title = parts[2] || null;
    } else if (mapper && typeof mapper === 'object' && !Array.isArray(mapper)) {
        const firstKey = (Object.keys(mapper)[0] || '').trim();
        if (firstKey) {
            const parts = firstKey.split('#$%');
            opt1_title = parts[0] || null;
            opt2_title = parts[1] || null;
            opt3_title = parts[2] || null;
        }
    }
    const oa = get('option_array');
    if (Array.isArray(oa) && oa.length > 0) {
        if (!opt1_title && oa[0]) opt1_title = oa[0].option_name || null;
        if (!opt2_title && oa[1]) opt2_title = oa[1].option_name || null;
        if (!opt3_title && oa[2]) opt3_title = oa[2].option_name || null;
    }

    // ── option_stock_data: JSON 파싱 후 정규화 ───────────────────────────
    let options_raw = [];
    const stockData = get('option_stock_data');
    let _parsed = null;
    if (typeof stockData === 'string' && stockData.trim()) {
        try { _parsed = JSON.parse(stockData); } catch(e) {}
    } else if (stockData && typeof stockData === 'object') {
        _parsed = stockData;
    }

    if (_parsed) {
        if (Array.isArray(_parsed)) {
            // 구 배열 형식 → item_code 없음 (Python 측에서 합성)
            options_raw = _parsed.map(item => ({ ...item, item_code: '' }));
        } else if (typeof _parsed === 'object') {
            // 신 오브젝트 형식 → key = item_code, opt1~3_name 직접 추출
            for (const key of Object.keys(_parsed)) {
                const item = _parsed[key];
                const origVals = Array.isArray(item.option_value_orginal) ? item.option_value_orginal : [];
                // 방어 코드: is_selling 누락/null → 'T'(판매 중), use_soldout 누락/null → 'F'(품절 아님)
                // || 연산자는 빈 문자열도 폴백시키므로 ?? 연산자(nullish coalescing) 사용
                const isSelling  = (item.is_selling  != null) ? String(item.is_selling)  : 'T';
                const useSoldout = (item.use_soldout != null) ? String(item.use_soldout) : 'F';
                options_raw.push({
                    item_code:    key,
                    opt1_name:    origVals[0] || '',
                    opt2_name:    origVals[1] || '',
                    opt3_name:    origVals[2] || '',
                    option_value: item.option_value || origVals.join('-'),
                    option_price: item.option_price || 0,
                    stock_number: item.stock_number  || 0,
                    is_selling:   isSelling,
                    use_soldout:  useSoldout,
                });
            }
        }
    }

    // option_array 폴백 (option_stock_data 없을 때)
    if (!options_raw.length && Array.isArray(oa)) {
        for (const grp of oa) {
            for (const v of (grp.option_value || [])) {
                options_raw.push({
                    item_code:       '',
                    option_value:    v.value || '',
                    option_price:    String(v.price_add || 0),
                    stock_number:    String(v.stock || 0),
                    option_disabled: (v.stock_display === 'F' || String(v.soldout).toUpperCase() === 'T')
                                     ? 'T' : 'F',
                });
            }
        }
    }

    // ── ld+json offers: option_code(sku) 보조 및 opt1~3_name 분리 폴백 ──
    // offers[].name 형식 예: "블루-S-기본" → opt1: 블루, opt2: S, opt3: 기본
    let offers_json = [];
    for (const script of document.querySelectorAll('script[type="application/ld+json"]')) {
        try {
            const data = JSON.parse(script.textContent || '');
            let offers = data.offers ?? null;
            if (offers && typeof offers === 'object' && !Array.isArray(offers)) {
                offers = offers.offers ?? [];
            }
            if (Array.isArray(offers) && offers.length > 0) {
                offers_json = offers.map(o => ({
                    sku:      String(o.sku || o['@id'] || '').trim(),
                    name:     String(o.name || '').trim(),
                    price:    parseFloat(String(o.price || 0)) || 0,
                    in_stock: String(o.availability || '').toLowerCase().includes('instock'),
                }));
                break;
            }
        } catch(e) {}
    }

    // ── 전체 품절 판별: is_soldout_icon 단독 사용 ────────────────────────
    // [최우선] is_soldout_icon = 'T' → 상품 강제 품절 (product.soldout=True)
    //          is_soldout_icon = 'F' 또는 미설정 → 개별 옵션 is_selling 으로 판단
    // aSoldoutDisplay / stock_number / use_soldout 은 판단 기준에서 제외
    const is_soldout_icon_val = get('is_soldout_icon');
    const is_soldout_icon_str = String(is_soldout_icon_val ?? '').toUpperCase().trim();
    const is_totally_sold_out = (is_soldout_icon_str === 'T');

    // ── 상세설명 HTML 처리 ──────────────────────────────────────────────
    const prdEl = document.getElementById('prdDetail');
    let description = '';
    if (prdEl) {
        description = prdEl.innerHTML
            .replace(/ec-data-src=/gi, 'src=')
            .replace(/\s*src="data:[^"]*"/gi, '')
            .trim();
    }

    return {
        name, image_url, base_price, custom_code,
        opt1_title, opt2_title, opt3_title,
        options_raw, offers_json,
        description,
        is_totally_sold_out,
        is_soldout_icon_str,   // 디버그용 원시값 ('T' | 'F' | '')
        exclusive_label,
    };
}
"""

# ──────────────────────────────────────────────────────────────────────────────
# 데이터 클래스
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ListingItem:
    url:          str
    wing_code:    str
    name:         str
    retail_price: int
    model:        str
    image_url:    str
    soldout:      bool


@dataclass
class ProductData:
    """parsing_wing_products 컬럼 (supabase.ts v5 기준)"""
    wing_code:            str
    name:                 str
    sub_name:             Optional[str]
    model:                str
    product_url:          str
    image_url:            str
    description:          str
    retail_price:         int            # 기본 판매가 (base_price)
    opt1_title:           Optional[str]
    opt2_title:           Optional[str]
    opt3_title:           Optional[str]  # 3단 옵션 타이틀
    soldout:              bool
    is_totally_sold_out:  bool


@dataclass
class OptionData:
    """parsing_wing_options 컬럼 (supabase.ts v5 기준)"""
    option_code:  str            # item_code 기반 고유 식별자
    opt1_name:    str
    opt2_name:    str
    opt3_name:    Optional[str]  # 3단 옵션명
    option_cond:  int
    stock_count:  int            # (구 option_stock)
    add_price:    int            # 기본가 대비 추가 금액 (구 retail_price - base_price)


# ──────────────────────────────────────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────────────────────────────────────

import json as _json


def _as_dict(v) -> dict:
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            parsed = _json.loads(v)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    if v is None:
        return {}
    try:
        return dict(v)
    except Exception:
        return {}


def _as_list(v) -> list:
    if isinstance(v, list):
        return [_as_dict(item) for item in v]
    if isinstance(v, str):
        try:
            parsed = _json.loads(v)
            if isinstance(parsed, list):
                return [_as_dict(item) for item in parsed]
        except Exception:
            pass
    return []


def _cond_label(cond: int) -> str:
    return "판매중" if cond == 1 else "품절"


def parse_price(text: str) -> int:
    digits = re.sub(r"[^\d]", "", str(text))
    return int(digits) if digits else 0


def extract_wing_code(url: str) -> Optional[str]:
    m = re.search(r"/product/[^/]+/(\d+)/", url)
    return m.group(1) if m else None


def _make_option_code(wing_code: str, item_code: str, opt1: str, opt2: str, opt3: str) -> str:
    """
    안정적인 option_code 생성.
    신 형식: item_code(option_stock_data의 키) 그대로 사용.
    구 형식 / 폴백: wing_code + opt 이름 조합으로 합성.
    """
    if item_code:
        return str(item_code).strip()
    parts = [wing_code]
    for p in [opt1, opt2, opt3]:
        cleaned = re.sub(r"[^\w가-힣]", "", str(p or "")).strip()
        if cleaned:
            parts.append(cleaned)
    return "-".join(parts)[:100]


def friendly_db_error(exc: Exception) -> str:
    msg = str(exc)
    if "10035" in msg or ("WinError" in msg and "10035" in msg):
        return f"소켓 오류 (WinError 10035): DB 동시 연결 초과 — {msg[:120]}"
    if "42P10" in msg:
        return (
            "DB 오류 42P10: ON CONFLICT 대상 컬럼이 UNIQUE 제약이 아닙니다. "
            "Supabase 대시보드에서 parsing_wing_products.wing_code 컬럼에 "
            "UNIQUE 제약을 설정해주세요."
        )
    if "23505" in msg:
        return "DB 오류 23505: 중복 키 위반 — 동일한 wing_code가 이미 존재합니다."
    if "23502" in msg:
        return "DB 오류 23502: NOT NULL 위반 — 필수 컬럼에 값이 없습니다."
    if "42703" in msg:
        return f"DB 오류 42703: 존재하지 않는 컬럼 — {msg}"
    if "42P01" in msg:
        return f"DB 오류 42P01: 테이블이 존재하지 않습니다 — {msg}"
    return f"DB 오류: {msg}"


def _is_retryable_error(exc: Exception) -> bool:
    """WinError 10035 (Windows 소켓 버퍼 초과) 등 일시적 소켓 오류 여부 확인."""
    msg = str(exc)
    return any(k in msg for k in ("10035", "WinError", "EAGAIN", "temporarily unavailable"))


def _db_retry(fn, *, max_retries: int = DB_MAX_RETRIES, delay: float = DB_RETRY_DELAY):
    """
    소켓 오류 발생 시 delay초 대기 후 최대 max_retries번 재시도하는 동기 DB 실행 래퍼.
    재시도 불가 오류(제약 위반 등)는 즉시 raise.
    """
    import time
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if _is_retryable_error(exc) and attempt < max_retries:
                log.warning(
                    "  DB 소켓 오류, %.1f초 후 재시도 (%d/%d): %s",
                    delay, attempt, max_retries, exc,
                )
                time.sleep(delay)
            else:
                raise
    raise last_exc  # 타입 체커 만족용 (실제 도달 불가)


# ──────────────────────────────────────────────────────────────────────────────
# 브라우저 컨텍스트
# ──────────────────────────────────────────────────────────────────────────────

async def create_browser_context(pw) -> BrowserContext:
    browser: Browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--lang=ko-KR",
        ],
    )
    ctx = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1920, "height": 1080},
        locale="ko-KR",
        timezone_id="Asia/Seoul",
        extra_http_headers={
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Referer": "https://winghouse.co.kr/",
        },
    )
    await ctx.add_init_script(STEALTH_SCRIPT)
    return ctx


async def setup_resource_blocking(page: Page) -> None:
    """이미지·폰트·CSS·미디어 로딩을 차단합니다 (텍스트/JSON 수집 전용)."""
    async def _handler(route):
        if route.request.resource_type in BLOCK_RESOURCE_TYPES:
            await route.abort()
        else:
            await route.continue_()
    await page.route("**/*", _handler)


# ──────────────────────────────────────────────────────────────────────────────
# 목록 페이지 스캔
# ──────────────────────────────────────────────────────────────────────────────

async def scan_listing_page(page: Page, url: str) -> list[ListingItem]:
    await page.goto(url, wait_until="domcontentloaded")
    try:
        await page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass

    raw = await page.evaluate(LISTING_JS)
    return [
        ListingItem(
            url          = r.get("url", ""),
            wing_code    = r.get("wing_code", ""),
            name         = r.get("name", ""),
            retail_price = r.get("retail_price", 0),
            model        = r.get("model", ""),
            image_url    = r.get("image_url", ""),
            soldout      = bool(r.get("soldout", False)),
        )
        for r in _as_list(raw)
        if r.get("name") and r.get("wing_code")
    ]


async def get_total_pages(page: Page) -> int:
    n = await page.evaluate(r"""
        () => {
            const nums = [...document.querySelectorAll('a[href*="page="]')]
                .map(a => { const m = a.href.match(/[?&]page=(\d+)/); return m ? +m[1] : 0; })
                .filter(n => n > 0);
            return nums.length ? Math.max(...nums) : 1;
        }
    """)
    return max(int(n or 1), 1)


# ──────────────────────────────────────────────────────────────────────────────
# API 기반 전체 목록 수집
# ──────────────────────────────────────────────────────────────────────────────

_SITE_ROOT = "https://winghouse.co.kr"


def _abs_url(path: str) -> str:
    if not path:
        return ""
    if path.startswith("//"):
        return "https:" + path
    if path.startswith("/"):
        return _SITE_ROOT + path
    return path


async def fetch_all_listings_via_api(
    ctx: BrowserContext,
    max_pages: int = 9999,
    per_page: int = 24,
    on_page: Optional[Callable[[int, int], None]] = None,
) -> list[ListingItem]:
    """
    카페24 ApiProductNormal JSON API를 페이지 단위로 호출하여
    전체 상품 목록을 수집합니다.
    """
    API_BASE = (
        "https://winghouse.co.kr/exec/front/Product/ApiProductNormal"
        f"?cate_no={CATEGORY_ID}&supplier_code=S0000000"
        f"&count={per_page}&bInitMore=F"
    )
    REQ_HEADERS = {
        "User-Agent": USER_AGENT,
        "Referer":    CATEGORY_URL,
        "Accept":     "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }

    seen: set[str] = set()
    all_items: list[ListingItem] = []

    for page in range(1, max_pages + 1):
        api_url = f"{API_BASE}&page={page}"
        try:
            resp = await ctx.request.get(api_url, headers=REQ_HEADERS)

            if not resp.ok:
                log.warning(f"API 응답 오류 (page={page}): HTTP {resp.status}")
                break

            body = await resp.text()

            try:
                payload = _json.loads(body)
            except Exception as e:
                log.error(
                    f"API JSON 파싱 실패 (page={page}): {e}\n"
                    f"응답 앞 500자: {body[:500]}"
                )
                break

            rtn_code = str(payload.get("rtn_code", ""))
            if rtn_code != "1000":
                log.warning(f"API 오류 코드 (page={page}): rtn_code={rtn_code}")
                break

            products: list = (payload.get("rtn_data") or {}).get("data") or []
            if not products:
                log.info(f"  page={page}: 상품 없음 → 수집 종료")
                break

            new_count = 0
            for p in products:
                wing_code = str(p.get("product_no") or "").strip()
                if not wing_code or wing_code in seen:
                    continue
                seen.add(wing_code)
                new_count += 1

                href         = _abs_url(p.get("link_product_detail") or "")
                image_url    = _abs_url(p.get("image_big") or p.get("image_medium") or "")
                name         = (
                    p.get("product_name_striptag") or
                    p.get("disp_product_name") or ""
                ).strip()
                retail_price = int(p.get("product_price") or 0)
                soldout      = bool(p.get("soldout_icon")) or not p.get("basket_display", True)

                all_items.append(ListingItem(
                    url=href,
                    wing_code=wing_code,
                    name=name,
                    retail_price=retail_price,
                    model="",
                    image_url=image_url,
                    soldout=soldout,
                ))

            log.info(f"  API page={page} — {new_count}건 추가 (누계 {len(all_items)}건)")

            if on_page:
                on_page(page, len(all_items))

            if new_count == 0:
                log.info("  → 신규 상품 없음, 수집 종료")
                break

        except Exception as exc:
            log.error(f"API 호출 실패 (page={page}): {exc}")
            break

        await asyncio.sleep(0.5)

    return all_items


# ──────────────────────────────────────────────────────────────────────────────
# 상세 페이지 파싱
# ──────────────────────────────────────────────────────────────────────────────

async def scrape_product_detail(
    page: Page, item: ListingItem
) -> tuple[ProductData, list[OptionData]]:
    """
    상세 페이지에서 JS 변수 우선 추출, 없으면 DOM 폴백.

    품절 판단 우선순위 (is_soldout_icon 및 is_selling 플래그 최우선):
      [상품 레벨] is_soldout_icon='T' → product.soldout=True (강제 품절)
              is_soldout_icon='F' (또는 미설정) → 개별 옵션 집계로 상품 상태 결정
                → 판매중(1) 옵션이 1개라도 있으면 product.soldout=False
                → 전체 품절(모두 2)이면 product.soldout=True
      [옵션 레벨] is_selling='T' → option_cond=1(판매중)
              is_selling='F' (또는 기타) → option_cond=2(품절)
              stock_number / use_soldout / aSoldoutDisplay 완전 무시
      ※ is_soldout_icon='T' 시 옵션 option_cond 강제 override 없음 (is_selling 결과 유지)

    추출 우선순위:
      1) JS 전역변수 (option_name_mapper → opt1~3_title, option_stock_data → 옵션목록)
      2) ld+json offers (option_code/sku, opt1~3_name 분리 '-' 기준, 폴백)
      3) option_array (옵션 데이터 최종 폴백)
    """
    await page.goto(item.url, wait_until="domcontentloaded")
    # networkidle 대기 제거: JS 인라인 변수는 DOMContentLoaded 이전에 정의됨

    js = _as_dict(await page.evaluate(DETAIL_JS))

    name                = js.get("name") or item.name
    image_url           = js.get("image_url") or item.image_url
    base_price          = int(js.get("base_price") or item.retail_price or 0)
    custom_code         = str(js.get("custom_code") or item.model or item.wing_code)
    opt1_title          = js.get("opt1_title") or None
    opt2_title          = js.get("opt2_title") or None
    opt3_title          = js.get("opt3_title") or None
    # is_totally_sold_out: is_soldout_icon='T' 일 때만 True → 상품 강제 품절
    is_totally_sold_out = bool(js.get("is_totally_sold_out", False))
    is_soldout_icon_str = str(js.get("is_soldout_icon_str") or "").strip()  # 디버그용
    exclusive_label     = str(js.get("exclusive_label") or "").strip() or None

    _desc_raw = js.get("description") or ""
    _desc_raw = _desc_raw.replace("ec-data-src=", "src=")
    _desc_raw = re.sub(r'\s*src="data:[^"]*"', "", _desc_raw, flags=re.IGNORECASE)
    description = _desc_raw.strip() or name

    options_raw = _as_list(js.get("options_raw") or [])
    offers_json = _as_list(js.get("offers_json") or [])

    # 진입 디버그: is_soldout_icon 원시값 출력
    log.debug(
        "[파싱시작] %s (wing_code=%s) — is_soldout_icon=%r → is_totally_sold_out=%s, 옵션 %d건",
        name, item.wing_code, is_soldout_icon_str, is_totally_sold_out, len(options_raw),
    )

    # ld+json offers → 옵션명(opt1-opt2-opt3) 기반 sku 맵 구성
    # sku는 option_code 보조용: option_stock_data item_code 없을 때 활용
    offer_by_name: dict[str, dict] = {}
    for off in offers_json:
        n = str(off.get("name") or "").strip()
        if n:
            offer_by_name[n] = off

    # ── 옵션 생성 ──────────────────────────────────────────────────────────
    options: list[OptionData] = []

    for raw in options_raw:
        item_code = str(raw.get("item_code") or "").strip()

        # opt1 / opt2 / opt3 이름 추출
        if raw.get("opt1_name") is not None:
            # 신 오브젝트 형식: opt1~3_name 직접 제공
            opt1_name = str(raw.get("opt1_name") or "").strip()
            opt2_name = str(raw.get("opt2_name") or "").strip()
            opt3_name = str(raw.get("opt3_name") or "").strip() or None
            add_price = parse_price(str(raw.get("option_price") or 0))
        else:
            # 구 배열 형식: option_value 문자열에서 #$% 분리
            opt_val   = str(raw.get("option_value") or "")
            parts     = opt_val.split("#$%")
            opt1_name = parts[0].strip()
            opt2_name = parts[1].strip() if len(parts) > 1 else ""
            opt3_name = parts[2].strip() if len(parts) > 2 else None
            add_price = parse_price(str(raw.get("option_price") or 0))

        # item_code 없으면 ld+json offers에서 sku 보조 시도
        # 옵션명 조합 "opt1-opt2[-opt3]" 으로 매칭
        if not item_code:
            candidate_names = [
                "-".join(filter(None, [opt1_name, opt2_name, opt3_name or ""])),
                "-".join(filter(None, [opt1_name, opt2_name])),
                opt1_name,
            ]
            for cand in candidate_names:
                off = offer_by_name.get(cand)
                if off:
                    item_code = str(off.get("sku") or "").strip()
                    # sku 기반 ld+json에서 opt 이름 분리 재시도
                    offer_name_parts = cand.split("-")
                    if len(offer_name_parts) >= 3 and not opt3_name:
                        opt3_name = offer_name_parts[2].strip() or None
                    break

        option_code = _make_option_code(
            item.wing_code, item_code, opt1_name, opt2_name, opt3_name or ""
        )

        # stock_number는 DB 저장용으로만 사용 — option_cond 결정에는 완전 무시
        stock_raw = str(raw.get("stock_number") or "0").strip()
        stock     = int(stock_raw) if stock_raw.isdigit() else 0

        # ── option_cond 결정 [is_soldout_icon 및 is_selling 플래그에 따른 상태 결정] ──
        #
        # [신 오브젝트 형식] is_selling 단독 기반 (최우선):
        #   is_selling='T' → 판매중(1)
        #   is_selling='F' (또는 기타) → 품절(2)
        #   use_soldout / stock_number : 완전 무시
        #
        # [구 배열 형식] option_disabled 기반 (is_selling 없음):
        #   option_disabled='F' → 판매중(1), 'T' → 품절(2)
        #   stock_number        : 동일하게 무시
        #
        if "is_selling" in raw:
            _is_sel = raw.get("is_selling")
            # 방어 기본값: is_selling 누락/None → 'T' (판매 중)
            selling     = str(_is_sel if _is_sel is not None else "T").upper() == "T"
            # is_selling='T' → 판매중(1), 그 외 → 품절(2)  (use_soldout 무시)
            option_cond = 1 if selling else 2
            log.debug(
                "  ├ [is_soldout_icon 및 is_selling 플래그에 따른 상태 결정]"
                " 옵션[%s] %s: is_selling=%r(%s) stock=%d(무시) use_soldout(무시) → %s",
                option_code, opt1_name,
                _is_sel, "T" if selling else "F",
                stock, _cond_label(option_cond),
            )
        else:
            # 구 배열 형식: option_disabled 단독 판단, stock 무시
            _opt_dis    = raw.get("option_disabled")
            disabled    = str(_opt_dis or "F").upper() == "T"
            option_cond = 2 if disabled else 1
            log.debug(
                "  ├ [is_soldout_icon 및 is_selling 플래그에 따른 상태 결정]"
                " 옵션[%s] %s: option_disabled=%r(%s) stock=%d(무시) → %s",
                option_code, opt1_name,
                _opt_dis, "T" if disabled else "F",
                stock, _cond_label(option_cond),
            )

        options.append(OptionData(
            option_code = option_code,
            opt1_name   = opt1_name or "기본",
            opt2_name   = opt2_name,
            opt3_name   = opt3_name,
            option_cond = option_cond,
            stock_count = stock,
            add_price   = add_price,
        ))

    # 옵션 없는 단독 구매 상품 → "기본 옵션" 단일 행
    # is_totally_sold_out(=is_soldout_icon='T') 또는 목록 페이지 soldout 신호 기반
    if not options:
        no_opt_cond = 2 if (is_totally_sold_out or item.soldout) else 1
        log.debug(
            "  ├ [is_soldout_icon 및 is_selling 플래그에 따른 상태 결정]"
            " 옵션[기본] is_soldout_icon='%s'(totally=%s) item.soldout=%s → %s",
            is_soldout_icon_str, is_totally_sold_out, item.soldout, _cond_label(no_opt_cond),
        )
        options.append(OptionData(
            option_code = f"{item.wing_code}-0001",
            opt1_name   = "기본 옵션",
            opt2_name   = "",
            opt3_name   = None,
            option_cond = no_opt_cond,
            stock_count = 0,
            add_price   = 0,
        ))

    # ── 최종 품절 판별 [is_soldout_icon 및 is_selling 플래그에 따른 상태 결정] ──
    #
    # [1순위] is_soldout_icon='F' → 상품 강제 품절 (product.soldout=True)
    #   옵션 option_cond 는 is_selling 플래그 결과 그대로 유지 (강제 덮어쓰기 없음)
    #
    # [2순위] is_soldout_icon='T' (또는 미설정)
    #   판매중(1) 옵션이 1개라도 있으면 → soldout=False
    #   전체 품절(모두 2)이면 → soldout=True
    #
    selling_opts = [o for o in options if o.option_cond == 1]
    soldout_opts = [o for o in options if o.option_cond == 2]

    if is_totally_sold_out:
        # ── is_soldout_icon='T': 상품 강제 품절 ──────────────────────────
        # 옵션 option_cond 는 is_selling 플래그 결과 그대로 유지 (강제 override 없음)
        all_soldout = True
        log.debug(
            "[최종판정] [is_soldout_icon 및 is_selling 플래그에 따른 상태 결정]"
            " %s (wing_code=%s) — is_soldout_icon='T'"
            ": 상품 강제 품절 (옵션 판매중 %d개 / 품절 %d개 / 전체 %d개)",
            name, item.wing_code, len(selling_opts), len(soldout_opts), len(options),
        )
    else:
        # ── is_soldout_icon='F': 개별 옵션 집계로 상품 상태 결정 ─────────
        all_soldout = len(selling_opts) == 0

        if all_soldout:
            log.debug(
                "[최종판정] [is_soldout_icon 및 is_selling 플래그에 따른 상태 결정]"
                " %s (wing_code=%s) — is_soldout_icon='%s'"
                ": 전체 품절 (판매중 0 / 품절 %d / 전체 %d)",
                name, item.wing_code, is_soldout_icon_str or "F",
                len(soldout_opts), len(options),
            )
        else:
            log.debug(
                "[최종판정] [is_soldout_icon 및 is_selling 플래그에 따른 상태 결정]"
                " %s (wing_code=%s) — is_soldout_icon='%s'"
                ": 판매중 (판매중 %d / 품절 %d / 전체 %d)",
                name, item.wing_code, is_soldout_icon_str or "F",
                len(selling_opts), len(soldout_opts), len(options),
            )

    product = ProductData(
        wing_code           = item.wing_code,
        name                = name,
        sub_name            = exclusive_label,
        model               = custom_code or item.wing_code,
        product_url         = item.url,
        image_url           = image_url,
        description         = description,
        retail_price        = base_price,
        opt1_title          = opt1_title,
        opt2_title          = opt2_title,
        opt3_title          = opt3_title,
        soldout             = all_soldout,
        is_totally_sold_out = is_totally_sold_out,
    )
    return product, options


# ──────────────────────────────────────────────────────────────────────────────
# 병렬 처리용 헬퍼
# ──────────────────────────────────────────────────────────────────────────────

async def scrape_with_new_page(
    ctx: BrowserContext,
    item: ListingItem,
    semaphore: asyncio.Semaphore,
    max_attempts: int = 2,
) -> tuple[ListingItem, Optional[ProductData], Optional[list[OptionData]], Optional[Exception]]:
    """
    Semaphore로 동시 처리 수를 제한하며 새 페이지를 열어 상세 페이지를 스크래핑합니다.
    리소스 차단을 적용하고, 완료(성공·실패) 후 페이지를 반드시 닫습니다.
    재시도는 semaphore 보유 상태에서 수행됩니다.
    """
    async with semaphore:
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            page = await ctx.new_page()
            try:
                await setup_resource_blocking(page)
                product, options = await scrape_product_detail(page, item)
                return item, product, options, None
            except Exception as exc:
                last_exc = exc
                log.debug(
                    "  [재시도] [%s] 스크래핑 실패 (시도 %d/%d): %s",
                    item.wing_code, attempt, max_attempts, exc,
                )
                if attempt < max_attempts:
                    await asyncio.sleep(2)
            finally:
                await page.close()
        return item, None, None, last_exc


async def save_product_async(
    product: ProductData,
    options: list[OptionData],
    on_status_change: Optional[Callable[[str], None]] = None,
) -> int:
    """단일 상품을 스레드 풀에서 비동기로 저장합니다 (save_products_batch 위임)."""
    loop = asyncio.get_running_loop()
    results = await loop.run_in_executor(
        None,
        lambda: save_products_batch([(product, options)], on_status_change),
    )
    return results[0] if results else 0


# ──────────────────────────────────────────────────────────────────────────────
# 배치 DB 처리 (일괄 조회 / 일괄 저장)
# ──────────────────────────────────────────────────────────────────────────────

def fetch_existing_for_batch(
    wing_codes: list[str],
) -> tuple[dict[str, dict], dict[str, dict[str, dict]]]:
    """
    여러 wing_code의 기존 상품·옵션 데이터를 최소 쿼리(2건)로 일괄 조회합니다.

    반환:
        existing_prods      : wing_code → {prod_cond, retail_price}
        existing_opts_by_wc : wing_code → {option_code → {option_cond, add_price,
                                                           stock_count, soldout_at, resale_at}}
    """
    existing_prods:      dict[str, dict]             = {}
    existing_opts_by_wc: dict[str, dict[str, dict]]  = {}

    if not wing_codes:
        return existing_prods, existing_opts_by_wc

    try:
        prod_resp = _db_retry(
            lambda: supabase.table("parsing_wing_products")
            .select("wing_code, prod_cond, retail_price")
            .in_("wing_code", wing_codes)
            .execute()
        )
        for r in _as_list(prod_resp.data or []):
            wc = str(r.get("wing_code") or "").strip()
            if wc:
                existing_prods[wc] = {
                    "prod_cond":    int(r.get("prod_cond")    or 1),
                    "retail_price": int(r.get("retail_price") or 0),
                }
    except Exception as exc:
        log.warning("배치 상품 조회 실패 (상품 맵 빈 채로 진행): %s", exc)

    try:
        opts_resp = _db_retry(
            lambda: supabase.table("parsing_wing_options")
            .select(
                "wing_code, option_code, option_cond, "
                "add_price, stock_count, soldout_at, resale_at"
            )
            .in_("wing_code", wing_codes)
            .execute()
        )
        for r in _as_list(opts_resp.data or []):
            wc   = str(r.get("wing_code")   or "").strip()
            code = str(r.get("option_code") or "").strip()
            if wc and code:
                existing_opts_by_wc.setdefault(wc, {})[code] = {
                    "option_cond": int(r.get("option_cond")  or 1),
                    "add_price":   int(r.get("add_price")    or 0),
                    "stock_count": int(r.get("stock_count")  or 0),
                    "soldout_at":  r.get("soldout_at"),
                    "resale_at":   r.get("resale_at"),
                }
    except Exception as exc:
        log.warning("배치 옵션 조회 실패 (옵션 맵 빈 채로 진행): %s", exc)

    return existing_prods, existing_opts_by_wc


def save_products_batch(
    items: list[tuple[ProductData, list[OptionData]]],
    on_status_change: Optional[Callable[[str], None]] = None,
) -> list[int]:
    """
    여러 상품을 배치로 저장합니다.

    DB 쿼리 수 (N개 상품 기준):
      상품 SELECT  1건 (기존 N건 → 1건으로 감소)
      옵션 SELECT  1건 (기존 N건 → 1건으로 감소)
      상품 UPSERT  1건 (bulk)
      옵션 DELETE  1건 (bulk, IN 조건)
      옵션 INSERT  1건 (bulk)
      mappings     1건 (신규 상품만, bulk)
    ─────────────────────────────────────────
    총 ~6건  (기존 N × 5건 → 6건으로 감소)

    데이터 유실 방지:
      옵션 DELETE 후 INSERT 실패 시 상품별 개별 재삽입 시도(fallback).

    반환: items 순서와 동일한 저장된 옵션 건수 리스트 (실패한 상품은 0)
    """
    if not items:
        return []

    import time
    now     = datetime.now(timezone.utc).isoformat()
    _notify = on_status_change or (lambda msg: log.info(msg))

    wing_codes: list[str] = [p.wing_code for p, _ in items]

    # ── 1. 기존 데이터 일괄 조회 ──────────────────────────────────────────────
    existing_prods, existing_opts_by_wc = fetch_existing_for_batch(wing_codes)

    # ── 2. 상품별 분석 및 row 구성 ────────────────────────────────────────────
    prod_rows:        list[dict]  = []
    all_opt_rows:     list[dict]  = []   # 전체 옵션 rows (wing_code 포함)
    new_wing_codes:   list[str]   = []   # 신규 상품 wing_code 목록
    saved_counts:     list[int]   = []   # 상품별 옵션 건수
    per_info:         list[dict]  = []   # 알림용 메타 정보

    for product, options in items:
        new_cond       = 2 if product.soldout else 1
        ex_prod        = existing_prods.get(product.wing_code)
        is_new         = ex_prod is None
        old_prod_cond  = 1 if is_new else int(ex_prod.get("prod_cond")    or 1)
        old_retail_prc = 0 if is_new else int(ex_prod.get("retail_price") or 0)

        prod_cond_changed  = (not is_new) and (old_prod_cond != new_cond)
        retail_prc_changed = (
            (not is_new)
            and (product.retail_price > 0)
            and (old_retail_prc != product.retail_price)
        )

        existing_opts = existing_opts_by_wc.get(product.wing_code, {})

        # 옵션 변경 감지
        opt_changes:        list[str] = []
        option_data_changed            = False
        for o in options:
            ex = existing_opts.get(o.option_code)
            if ex is None:
                continue
            parts     = [o.opt1_name]
            if o.opt2_name: parts.append(o.opt2_name)
            if o.opt3_name: parts.append(o.opt3_name)
            opt_label = "/".join(parts)
            if ex["option_cond"] != o.option_cond:
                option_data_changed = True
                ts_note = " → soldout_at 기록" if o.option_cond == 2 else " → resale_at 기록"
                forced  = " [is_soldout_icon=F]" if (product.is_totally_sold_out and o.option_cond == 2) else ""
                opt_changes.append(
                    f"[옵션상태변경] {product.name} [{opt_label}]: "
                    f"{_cond_label(ex['option_cond'])} → {_cond_label(o.option_cond)}"
                    f"{forced}{ts_note}"
                )
            if ex["add_price"] != o.add_price:
                option_data_changed = True
                opt_changes.append(
                    f"[옵션가변경] {product.name} [{opt_label}]: "
                    f"+{ex['add_price']:,}원 → +{o.add_price:,}원"
                )
            if ex["stock_count"] != o.stock_count:
                option_data_changed = True
                opt_changes.append(
                    f"[재고변경] {product.name} [{opt_label}]: "
                    f"{ex['stock_count']}개 → {o.stock_count}개"
                )

        any_changed = is_new or prod_cond_changed or retail_prc_changed or option_data_changed

        # 상품 row
        prod_row: dict = {
            "wing_code":    product.wing_code,
            "name":         product.name,
            "sub_name":     product.sub_name,
            "model":        product.model,
            "product_url":  product.product_url,
            "image_url":    product.image_url,
            "description":  product.description,
            "retail_price": product.retail_price,
            "opt1_title":   product.opt1_title,
            "opt2_title":   product.opt2_title,
            "opt3_title":   product.opt3_title,
            "prod_cond":    new_cond,
            "parsing_at":   now,
        }
        if any_changed:
            prod_row["update_at"] = now
        if retail_prc_changed:
            prod_row["prev_retail_price"] = old_retail_prc
            prod_row["price_updated_at"]  = now
        if prod_cond_changed:
            if old_prod_cond == 1 and new_cond == 2:
                prod_row["soldout_at"] = now
            elif old_prod_cond == 2 and new_cond == 1:
                prod_row["resale_at"]  = now
        prod_rows.append(prod_row)

        if is_new:
            new_wing_codes.append(product.wing_code)

        # 옵션 rows
        opt_rows_for_product: list[dict] = []
        for o in options:
            ex           = existing_opts.get(o.option_code) or {}
            old_opt_cond = int(ex.get("option_cond") or 1)
            soldout_at: Optional[str] = ex.get("soldout_at")
            resale_at:  Optional[str] = ex.get("resale_at")

            if ex and old_opt_cond != o.option_cond:
                if old_opt_cond == 1 and o.option_cond == 2:
                    soldout_at = now
                elif old_opt_cond == 2 and o.option_cond == 1:
                    resale_at = now
            elif not ex and o.option_cond == 2:
                soldout_at = now

            opt_row: dict = {
                "wing_code":   product.wing_code,
                "option_code": o.option_code,
                "opt1_name":   o.opt1_name,
                "opt2_name":   o.opt2_name or None,
                "opt3_name":   o.opt3_name or None,
                "option_cond": o.option_cond,
                "stock_count": o.stock_count,
                "soldout_at":  soldout_at,
                "resale_at":   resale_at,
            }
            if o.add_price:
                opt_row["add_price"] = o.add_price
            opt_rows_for_product.append(opt_row)

        all_opt_rows.extend(opt_rows_for_product)
        saved_counts.append(len(opt_rows_for_product))
        per_info.append({
            "product":            product,
            "options":            options,
            "is_new":             is_new,
            "old_prod_cond":      old_prod_cond,
            "old_retail_prc":     old_retail_prc,
            "new_cond":           new_cond,
            "prod_cond_changed":  prod_cond_changed,
            "retail_prc_changed": retail_prc_changed,
            "opt_changes":        opt_changes,
            "opt_rows":           opt_rows_for_product,
        })

    # ── 3. 상품 bulk UPSERT ────────────────────────────────────────────────────
    _db_retry(
        lambda: supabase.table("parsing_wing_products")
        .upsert(prod_rows, on_conflict="wing_code")
        .execute()
    )

    # ── 4. 옵션 bulk DELETE ────────────────────────────────────────────────────
    _db_retry(
        lambda: supabase.table("parsing_wing_options")
        .delete()
        .in_("wing_code", wing_codes)
        .execute()
    )

    # ── 5. 옵션 bulk INSERT (실패 시 상품별 개별 fallback으로 데이터 유실 방지) ──
    if all_opt_rows:
        try:
            _db_retry(
                lambda: supabase.table("parsing_wing_options")
                .insert(all_opt_rows)
                .execute()
            )
        except Exception as bulk_exc:
            log.warning("옵션 bulk INSERT 실패, 상품별 개별 삽입으로 전환: %s", bulk_exc)
            for info in per_info:
                rows = info["opt_rows"]
                if not rows:
                    continue
                wc = info["product"].wing_code
                try:
                    _db_retry(
                        lambda r=rows: supabase.table("parsing_wing_options")
                        .insert(r)
                        .execute()
                    )
                except Exception as ind_exc:
                    log.error("  [%s] 개별 옵션 INSERT 실패: %s", wc, ind_exc)
                    # saved_counts에서 해당 상품 카운트를 0으로 표시
                    idx = wing_codes.index(wc)
                    saved_counts[idx] = 0

    # ── 6. product_external_mappings (신규 상품 bulk INSERT) ──────────────────
    if new_wing_codes:
        try:
            em_resp = _db_retry(
                lambda: supabase.table("product_external_mappings")
                .select("external_code")
                .in_("external_code", new_wing_codes)
                .eq("provider_name", "winghouse")
                .execute()
            )
            already_mapped = {
                str(r.get("external_code") or "")
                for r in _as_list(em_resp.data or [])
            }
            mapping_rows = [
                {
                    "external_code": wc,
                    "provider_name": "winghouse",
                    "status":        "pending",
                    "created_at":    now,
                }
                for wc in new_wing_codes
                if wc not in already_mapped
            ]
            if mapping_rows:
                _db_retry(
                    lambda: supabase.table("product_external_mappings")
                    .insert(mapping_rows)
                    .execute()
                )
        except Exception as e:
            log.warning("product_external_mappings 배치 삽입 실패: %s", e)

    # ── 7. 변경 알림 출력 ─────────────────────────────────────────────────────
    for info in per_info:
        product            = info["product"]
        options            = info["options"]
        is_new             = info["is_new"]
        old_prod_cond      = info["old_prod_cond"]
        old_retail_prc     = info["old_retail_prc"]
        new_cond           = info["new_cond"]
        prod_cond_changed  = info["prod_cond_changed"]
        retail_prc_changed = info["retail_prc_changed"]
        opt_changes        = info["opt_changes"]

        if is_new:
            soldout_cnt = sum(1 for o in options if o.option_cond == 2)
            forced_note = " [is_soldout_icon=F 강제품절]" if product.is_totally_sold_out else ""
            _notify(
                f"[신규등록] {product.name} (wing_code={product.wing_code}): "
                f"옵션 {len(options)}건 (품절 {soldout_cnt}건){forced_note}, "
                f"기본가 {product.retail_price:,}원"
            )
        if prod_cond_changed:
            if product.is_totally_sold_out and new_cond == 2:
                reason = " [is_soldout_icon=F 강제품절]"
            elif new_cond == 2:
                reason = " [모든 옵션 품절]"
            else:
                reason = " [재판매 전환]"
            _notify(
                f"[상품상태변경] {product.name} (wing_code={product.wing_code}): "
                f"{_cond_label(old_prod_cond)} → {_cond_label(new_cond)}{reason}"
            )
        if retail_prc_changed:
            _notify(
                f"[가격변경] {product.name} (wing_code={product.wing_code}): "
                f"{old_retail_prc:,}원 → {product.retail_price:,}원 "
                f"(prev_retail_price 기록, price_updated_at 갱신)"
            )
        for msg in opt_changes:
            _notify(msg)

    return saved_counts


# ──────────────────────────────────────────────────────────────────────────────
# Supabase 저장

def save_product_and_options(
    product: ProductData,
    options: list[OptionData],
    on_status_change: Optional[Callable[[str], None]] = None,
) -> int:
    """
    선조회(Select) → 비교 → 조건부 upsert 전략:

    상품(parsing_wing_products):
      - retail_price 변경 → prev_retail_price = 기존값, price_updated_at = now
      - prod_cond 변경 (1→2) → soldout_at = now
      - prod_cond 변경 (2→1) → resale_at = now
      - 어떤 변경이든 있으면 → update_at = now

    옵션(parsing_wing_options):
      - 기존 옵션 전체 삭제 후 재삽입 (soldout_at / resale_at 기존 값 보존)
      - option_cond 변경 (1→2) → soldout_at = now
      - option_cond 변경 (2→1) → resale_at = now

    product_external_mappings:
      - 신규 상품이고 wing_code 미등록 시 status='pending' 자동 생성

    on_status_change: 변경 메시지 콜백 (None이면 log.info)
    반환: 저장된 옵션 건수
    """
    now      = datetime.now(timezone.utc).isoformat()
    _notify  = on_status_change or (lambda msg: log.info(msg))
    new_cond = 2 if product.soldout else 1

    # ── 1. 기존 상품 조회 ─────────────────────────────────────────────────
    ex_prod_resp = (
        supabase.table("parsing_wing_products")
        .select("wing_code, prod_cond, retail_price")
        .eq("wing_code", product.wing_code)
        .limit(1)
        .execute()
    )
    ex_prod_rows   = _as_list(ex_prod_resp.data) if ex_prod_resp.data else []
    existing_prod  = ex_prod_rows[0] if ex_prod_rows else None
    is_new         = existing_prod is None
    old_prod_cond  = 1 if is_new else int(existing_prod.get("prod_cond") or 1)
    old_retail_prc = 0 if is_new else int(existing_prod.get("retail_price") or 0)

    prod_cond_changed  = (not is_new) and (old_prod_cond != new_cond)
    retail_prc_changed = (
        (not is_new)
        and (product.retail_price > 0)
        and (old_retail_prc != product.retail_price)
    )

    # ── 2. 기존 옵션 조회 ─────────────────────────────────────────────────
    # {option_code: {option_cond, add_price, stock_count, soldout_at, resale_at}}
    existing_opts: dict[str, dict] = {}
    ex_opts_resp = (
        supabase.table("parsing_wing_options")
        .select("option_code, option_cond, add_price, stock_count, soldout_at, resale_at")
        .eq("wing_code", product.wing_code)
        .execute()
    )
    for r in _as_list(ex_opts_resp.data if ex_opts_resp.data else []):
        code = str(r.get("option_code") or "").strip()
        if code:
            existing_opts[code] = {
                "option_cond": int(r.get("option_cond")  or 1),
                "add_price":   int(r.get("add_price")    or 0),
                "stock_count": int(r.get("stock_count")  or 0),
                "soldout_at":  r.get("soldout_at"),
                "resale_at":   r.get("resale_at"),
            }

    # ── 3. 옵션 변경 사항 수집 ────────────────────────────────────────────
    opt_changes: list[str] = []
    option_data_changed    = False

    for o in options:
        ex = existing_opts.get(o.option_code)
        if ex is None:
            continue

        parts = [o.opt1_name]
        if o.opt2_name:
            parts.append(o.opt2_name)
        if o.opt3_name:
            parts.append(o.opt3_name)
        opt_label = "/".join(parts)

        if ex["option_cond"] != o.option_cond:
            option_data_changed = True
            # 상태 전환 방향에 따른 타임스탬프 안내 포함
            ts_note = " → soldout_at 기록" if o.option_cond == 2 else " → resale_at 기록"
            forced  = " [is_soldout_icon=F]" if (product.is_totally_sold_out and o.option_cond == 2) else ""
            opt_changes.append(
                f"[옵션상태변경] {product.name} [{opt_label}]: "
                f"{_cond_label(ex['option_cond'])} → {_cond_label(o.option_cond)}"
                f"{forced}{ts_note}"
            )

        if ex["add_price"] != o.add_price:
            option_data_changed = True
            opt_changes.append(
                f"[옵션가변경] {product.name} [{opt_label}]: "
                f"+{ex['add_price']:,}원 → +{o.add_price:,}원"
            )

        if ex["stock_count"] != o.stock_count:
            option_data_changed = True
            opt_changes.append(
                f"[재고변경] {product.name} [{opt_label}]: "
                f"{ex['stock_count']}개 → {o.stock_count}개"
            )

    # ── 4. update_at 조건부 결정 ─────────────────────────────────────────
    any_changed = is_new or prod_cond_changed or retail_prc_changed or option_data_changed

    # ── 5. 상품 upsert ─────────────────────────────────────────────────────
    prod_row: dict = {
        "wing_code":    product.wing_code,
        "name":         product.name,
        "sub_name":     product.sub_name,
        "model":        product.model,
        "product_url":  product.product_url,
        "image_url":    product.image_url,
        "description":  product.description,
        "retail_price": product.retail_price,
        "opt1_title":   product.opt1_title,
        "opt2_title":   product.opt2_title,
        "opt3_title":   product.opt3_title,
        "prod_cond":    new_cond,
        "parsing_at":   now,
    }

    if any_changed:
        prod_row["update_at"] = now

    # 가격 변경 기록
    if retail_prc_changed:
        prod_row["prev_retail_price"] = old_retail_prc
        prod_row["price_updated_at"]  = now

    # 품절 상태 전환 타임스탬프
    if prod_cond_changed:
        if old_prod_cond == 1 and new_cond == 2:
            prod_row["soldout_at"] = now
        elif old_prod_cond == 2 and new_cond == 1:
            prod_row["resale_at"]  = now

    _db_retry(
        lambda: supabase.table("parsing_wing_products")
        .upsert(prod_row, on_conflict="wing_code")
        .execute()
    )

    # ── 6. product_external_mappings (신규 상품만) ────────────────────────
    if is_new:
        try:
            em_check = _db_retry(
                lambda: supabase.table("product_external_mappings")
                .select("id")
                .eq("external_code", product.wing_code)
                .eq("provider_name", "winghouse")
                .limit(1)
                .execute()
            )
            if not _as_list(em_check.data if em_check.data else []):
                _db_retry(
                    lambda: supabase.table("product_external_mappings").insert({
                        "external_code": product.wing_code,
                        "provider_name": "winghouse",
                        "status":        "pending",
                        "created_at":    now,
                    }).execute()
                )
        except Exception as e:
            log.warning(f"product_external_mappings 삽입 실패 ({product.wing_code}): {e}")

    # ── 7. 변경 알림 출력 ────────────────────────────────────────────────
    if is_new:
        soldout_cnt = sum(1 for o in options if o.option_cond == 2)
        forced_note = " [is_soldout_icon=F 강제품절]" if product.is_totally_sold_out else ""
        _notify(
            f"[신규등록] {product.name} (wing_code={product.wing_code}): "
            f"옵션 {len(options)}건 (품절 {soldout_cnt}건){forced_note}, "
            f"기본가 {product.retail_price:,}원"
        )

    if prod_cond_changed:
        # 상태 변경 원인 분류
        if product.is_totally_sold_out and new_cond == 2:
            reason = " [is_soldout_icon=F 강제품절]"
        elif new_cond == 2:
            reason = " [모든 옵션 품절]"
        else:
            reason = " [재판매 전환]"
        _notify(
            f"[상품상태변경] {product.name} (wing_code={product.wing_code}): "
            f"{_cond_label(old_prod_cond)} → {_cond_label(new_cond)}{reason}"
        )

    if retail_prc_changed:
        _notify(
            f"[가격변경] {product.name} (wing_code={product.wing_code}): "
            f"{old_retail_prc:,}원 → {product.retail_price:,}원 "
            f"(prev_retail_price 기록, price_updated_at 갱신)"
        )

    for msg in opt_changes:
        _notify(msg)

    # ── 8. 옵션 삭제 후 재삽입 (soldout_at / resale_at 보존) ─────────────
    _db_retry(
        lambda: supabase.table("parsing_wing_options")
        .delete()
        .eq("wing_code", product.wing_code)
        .execute()
    )

    opt_rows: list[dict] = []
    for o in options:
        ex           = existing_opts.get(o.option_code) or {}
        old_opt_cond = int(ex.get("option_cond") or 1)

        soldout_at: Optional[str] = ex.get("soldout_at")
        resale_at:  Optional[str] = ex.get("resale_at")

        if ex and old_opt_cond != o.option_cond:
            if old_opt_cond == 1 and o.option_cond == 2:
                soldout_at = now
            elif old_opt_cond == 2 and o.option_cond == 1:
                resale_at = now
        elif not ex and o.option_cond == 2:
            soldout_at = now

        opt_row: dict = {
            "wing_code":    product.wing_code,
            "option_code":  o.option_code,
            "opt1_name":    o.opt1_name,
            "opt2_name":    o.opt2_name or None,
            "opt3_name":    o.opt3_name or None,
            "option_cond":  o.option_cond,
            "stock_count":  o.stock_count,
            "soldout_at":   soldout_at,
            "resale_at":    resale_at,
        }
        if o.add_price:
            opt_row["add_price"] = o.add_price
        opt_rows.append(opt_row)

    if opt_rows:
        _db_retry(
            lambda: supabase.table("parsing_wing_options").insert(opt_rows).execute()
        )

    return len(opt_rows)


def update_soldout_status(wing_code: str, soldout: bool) -> None:
    """
    품절 상태 빠른 업데이트 (상세 페이지 방문 없음).
    parsing_wing_products 및 해당 wing_code의 모든 옵션을 동시 업데이트.
    """
    now      = datetime.now(timezone.utc).isoformat()
    new_cond = 2 if soldout else 1

    prod_row: dict = {
        "prod_cond": new_cond,
        "update_at": now,
    }
    if soldout:
        prod_row["soldout_at"] = now
    else:
        prod_row["resale_at"]  = now

    _db_retry(
        lambda: supabase.table("parsing_wing_products")
        .update(prod_row)
        .eq("wing_code", wing_code)
        .execute()
    )

    opt_row: dict = {"option_cond": new_cond}
    if soldout:
        opt_row["soldout_at"] = now
    else:
        opt_row["resale_at"]  = now

    _db_retry(
        lambda: supabase.table("parsing_wing_options")
        .update(opt_row)
        .eq("wing_code", wing_code)
        .execute()
    )


# ──────────────────────────────────────────────────────────────────────────────
# 터미널 요약
# ──────────────────────────────────────────────────────────────────────────────

def print_summary(total: int, success: int, fail: int, opt_total: int, elapsed: float) -> None:
    sep = "─" * 52
    print(f"\n{sep}")
    print("  윙하우스 파싱 완료 요약")
    print(sep)
    print(f"  처리 상품  : {total:>5}건")
    print(f"  성공       : {success:>5}건  ({success / max(total, 1) * 100:.1f}%)")
    print(f"  실패       : {fail:>5}건")
    print(f"  저장 옵션  : {opt_total:>5}건")
    print(f"  소요 시간  : {elapsed:>5.1f}초")
    print(sep)


# ──────────────────────────────────────────────────────────────────────────────
# CLI 실행
# ──────────────────────────────────────────────────────────────────────────────

async def run(
    max_pages:   int = 9999,
    concurrency: int = DETAIL_CONCURRENCY,
) -> None:
    """
    CLI 진입점.

    Phase 1: API 루프로 전체 상품 wing_code 수집
    Phase 2: Semaphore(concurrency) 기반 병렬 스크래핑
             → DB_BATCH_SIZE 단위 save_products_batch 배치 저장
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    started   = datetime.now(timezone.utc)
    total = success = fail = opt_total = 0

    async with async_playwright() as pw:
        ctx = await create_browser_context(pw)

        log.info("Phase 1 — API 루프 목록 수집 시작")
        all_listings = await fetch_all_listings_via_api(ctx, max_pages=max_pages)
        log.info(f"Phase 1 완료 — 총 {len(all_listings)}건")

        log.info(
            f"Phase 2 — 상품 상세 파싱 (동시 {concurrency}개) + "
            f"DB 배치 저장 ({DB_BATCH_SIZE}개 단위)"
        )
        semaphore         = asyncio.Semaphore(concurrency)
        scrape_batch_size = concurrency * 2   # 스크래핑 배치: semaphore×2
        loop              = asyncio.get_running_loop()

        # 스크래핑 완료 결과를 누적하고 DB_BATCH_SIZE마다 저장
        pending_save: list[tuple[ListingItem, ProductData, list[OptionData]]] = []

        async def _flush_pending():
            nonlocal total, success, fail, opt_total
            if not pending_save:
                return
            try:
                saved_counts = await loop.run_in_executor(
                    None,
                    lambda items=list(pending_save): save_products_batch(
                        [(p, o) for _, p, o in items]
                    ),
                )
                for (item, product, options), saved in zip(pending_save, saved_counts):
                    total     += 1
                    if saved == 0 and options:
                        # 옵션이 있었는데 0이면 저장 실패로 간주
                        fail  += 1
                        log.warning(f"  NG [{item.wing_code}] 옵션 저장 실패")
                    else:
                        opt_total += saved
                        success   += 1
                        min_price  = min(
                            (product.retail_price + o.add_price for o in options
                             if product.retail_price + o.add_price > 0),
                            default=product.retail_price,
                        )
                        log.info(
                            f"  [수집완료] {product.name} "
                            f"(기본가: {product.retail_price:,}원, "
                            f"최저가: {min_price:,}원, "
                            f"상태: {_cond_label(2 if product.soldout else 1)}, "
                            f"옵션: {saved}건)"
                        )
            except Exception as exc:
                log.error(f"  배치 DB 저장 실패 ({len(pending_save)}건): {friendly_db_error(exc)}")
                fail += len(pending_save)
            finally:
                pending_save.clear()

        for batch_start in range(0, len(all_listings), scrape_batch_size):
            batch = all_listings[batch_start : batch_start + scrape_batch_size]

            scrape_results = await asyncio.gather(
                *[scrape_with_new_page(ctx, item, semaphore) for item in batch],
                return_exceptions=True,
            )

            for item, result in zip(batch, scrape_results):
                if isinstance(result, Exception):
                    fail += 1
                    log.warning(f"  NG [{item.wing_code}] {friendly_db_error(result)}")
                    continue
                _, product, options, err = result
                if err is not None:
                    fail += 1
                    log.warning(f"  NG [{item.wing_code}] {friendly_db_error(err)}")
                    continue
                pending_save.append((item, product, options))

            # DB_BATCH_SIZE 이상 누적되면 배치 저장
            if len(pending_save) >= DB_BATCH_SIZE:
                await _flush_pending()

        # 나머지 잔여분 저장
        await _flush_pending()
        await ctx.close()

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print_summary(total, success, fail, opt_total, elapsed)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="윙하우스 상품 파싱")
    ap.add_argument("--pages",       type=int, default=1,
                    help="파싱할 페이지 수 (기본: 1)")
    ap.add_argument("--concurrency", type=int, default=DETAIL_CONCURRENCY,
                    help=f"동시 처리 수 (기본: {DETAIL_CONCURRENCY}, 권장 3~5)")
    args = ap.parse_args()
    asyncio.run(run(max_pages=args.pages, concurrency=min(args.concurrency, 5)))
