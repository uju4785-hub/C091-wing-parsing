"""
winghouse_parser.py  —  윙하우스 상세 페이지 파싱 모듈 v3

JS 변수 우선 추출 전략:
  var product_name        → name
  ld+json image[0]        → image_url  (폴백: og:image)
  var option_name_mapper  → opt1_title / opt2_title  (키에 #$% 구분자 사용)
  var option_stock_data   → 옵션 목록 (option_value·option_price·stock_number)
  #prdDetail innerHTML    → description (ec-data-src → src 치환)

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
    // window 변수를 안전하게 읽는 헬퍼
    const get = (key) => {
        try { return window[key] !== undefined ? window[key] : null; }
        catch(e) { return null; }
    };

    // ── 기본 정보 ─────────────────────────────────────────────────────────
    const name = get('product_name') ||
        (document.querySelector('.headingArea h2, h2.name, #contents h2') || {}).innerText?.trim() || '';

    // ── 대표 이미지: ld+json image[0] 우선, og:image 폴백 ─────────────────
    let image_url = '';
    // 1) <script type="application/ld+json"> 내 "image" 배열의 첫 번째 항목
    for (const script of document.querySelectorAll('script[type="application/ld+json"]')) {
        try {
            const data = JSON.parse(script.textContent || '');
            const imgs = data.image ?? (Array.isArray(data['@graph']) ? (data['@graph'][0] || {}).image : null) ?? [];
            const first = Array.isArray(imgs) ? imgs[0] : (typeof imgs === 'string' ? imgs : '');
            if (first) { image_url = first; break; }
        } catch(e) {}
    }
    // 2) <meta property="og:image"> 폴백
    if (!image_url) {
        const og = document.querySelector('meta[property="og:image"]');
        image_url = og ? (og.getAttribute('content') || '') : '';
    }
    // 3) //로 시작하는 프로토콜 상대 경로 → https: 붙이기
    if (image_url.startsWith('//')) image_url = 'https:' + image_url;

    const base_price = parseInt(
        String(get('product_price') || '0').replace(/[^0-9]/g, '')
    ) || 0;

    // ── 자체 상품코드: JS 변수 우선, DOM 테이블 폴백 ─────────────────────
    let custom_code = String(get('product_custom_code') || get('product_code') || '');
    if (!custom_code) {
        // <th> 텍스트가 "상품코드"인 행의 <td> 텍스트를 추출
        for (const th of document.querySelectorAll('th')) {
            if ((th.textContent || '').includes('상품코드')) {
                const tr = th.closest('tr');
                const td = tr ? tr.querySelector('td') : th.nextElementSibling;
                const val = td ? td.textContent.trim() : '';
                if (val) { custom_code = val; break; }
            }
        }
    }

    // ── 옵션 타이틀: option_name_mapper ──────────────────────────────────
    //   문자열 형식(신): '색상#$%사이즈'
    //   오브젝트 형식(구): {"색상#$%사이즈": {...}, ...}
    let opt1_title = null, opt2_title = null;
    const mapper = get('option_name_mapper');
    if (typeof mapper === 'string' && mapper.trim()) {
        const parts = mapper.split('#$%');
        opt1_title = parts[0] || null;
        opt2_title = parts[1] || null;
    } else if (mapper && typeof mapper === 'object' && !Array.isArray(mapper)) {
        const firstKey = (Object.keys(mapper)[0] || '').trim();
        if (firstKey) {
            const parts = firstKey.split('#$%');
            opt1_title = parts[0] || null;
            opt2_title = parts[1] || null;
        }
    }
    // option_array 폴백 (opt title 보조)
    const oa = get('option_array');
    if (Array.isArray(oa) && oa.length > 0) {
        if (!opt1_title && oa[0]) opt1_title = oa[0].option_name || null;
        if (!opt2_title && oa[1]) opt2_title = oa[1].option_name || null;
    }

    // ── option_stock_data: JSON 파싱 후 정규화 ───────────────────────────
    //   오브젝트 형식(신): {코드: {option_value_orginal, option_price, stock_number, is_selling, use_soldout, option_value}, ...}
    //   배열 형식(구):     [{option_value, option_price, stock_number, option_disabled}, ...]
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
            // 구 배열 형식 → 그대로 사용 (Python 측에서 option_value #$% 분리)
            options_raw = _parsed;
        } else if (typeof _parsed === 'object') {
            // 신 오브젝트 형식 → 정규화
            for (const key of Object.keys(_parsed)) {
                const item = _parsed[key];
                const origVals = Array.isArray(item.option_value_orginal) ? item.option_value_orginal : [];
                options_raw.push({
                    opt1_name:   origVals[0] || '',
                    opt2_name:   origVals[1] || '',
                    option_value: item.option_value || origVals.join('-'),
                    option_price: item.option_price || 0,
                    stock_number: item.stock_number  || 0,
                    is_selling:   item.is_selling    || 'F',
                    use_soldout:  item.use_soldout   || 'F',
                });
            }
        }
    }

    // option_array 폴백 (option_stock_data 없을 때)
    if (!options_raw.length && Array.isArray(oa)) {
        for (const grp of oa) {
            for (const v of (grp.option_value || [])) {
                options_raw.push({
                    option_value:    v.value || '',
                    option_price:    String(v.price_add || 0),
                    stock_number:    String(v.stock || 0),
                    option_disabled: (v.stock_display === 'F' || String(v.soldout).toUpperCase() === 'T')
                                     ? 'T' : 'F',
                });
            }
        }
    }

    // ── 상세설명 HTML 처리 ──────────────────────────────────────────────
    // 1단계: ec-data-src="실제URL" → src="실제URL"
    // 2단계: 남아있는 src="data:image/...;base64,..." 제거 (중복 썸네일)
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
        opt1_title, opt2_title,
        options_raw, description,
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
    # parsing_wing_products 실제 컬럼만 포함 (supabase.ts Row 기준)
    wing_code:   str
    name:        str
    sub_name:    Optional[str]
    model:       str
    product_url: str
    image_url:   str
    description: str
    opt1_title:  Optional[str]
    opt2_title:  Optional[str]
    soldout:     bool


@dataclass
class OptionData:
    # parsing_wing_options 실제 컬럼만 포함 (supabase.ts Row 기준)
    # option_name 컬럼은 DB에 존재하지 않음 — opt1_name / opt2_name 으로 저장
    opt1_name:    str
    opt2_name:    str
    option_cond:  int
    option_stock: int
    cost_price:   int
    retail_price: int


# ──────────────────────────────────────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────────────────────────────────────

import json as _json


def _as_dict(v) -> dict:
    """
    Supabase 응답 행 또는 JS 평가 결과를 안전하게 dict로 변환.

    supabase-py 버전에 따라 resp.data[0]이 dict 또는 JSON 문자열로 올 수 있으며,
    page.evaluate() 결과도 드물게 직렬화된 문자열로 반환될 수 있습니다.
    """
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
    """Supabase/JS 결과를 안전하게 list[dict]로 변환."""
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
    """option_cond / prod_cond 값을 사람이 읽을 수 있는 레이블로 변환."""
    return "판매중" if cond == 1 else "품절"


def parse_price(text: str) -> int:
    digits = re.sub(r"[^\d]", "", str(text))
    return int(digits) if digits else 0


def extract_wing_code(url: str) -> Optional[str]:
    m = re.search(r"/product/[^/]+/(\d+)/", url)
    return m.group(1) if m else None


def friendly_db_error(exc: Exception) -> str:
    """PostgreSQL 에러 코드를 사용자 친화적 메시지로 변환."""
    msg = str(exc)
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


# ──────────────────────────────────────────────────────────────────────────────
# 목록 페이지 스캔
# ──────────────────────────────────────────────────────────────────────────────

async def scan_listing_page(page: Page, url: str) -> list[ListingItem]:
    """카테고리 페이지 한 장에서 ListingItem 목록을 반환."""
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
    """현재 페이지의 페이지네이션에서 마지막 페이지 번호를 반환."""
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

# 실제 API 응답 구조 (확인 완료):
# {
#   "rtn_code": "1000",
#   "rtn_data": {
#     "data": [
#       {
#         "product_no": 4855,
#         "link_product_detail": "/product/.../4855/category/134/display/1/",
#         "product_name_striptag": "상품명 (태그 제거됨)",
#         "product_price": 23000,
#         "image_big": "//ecimg.cafe24img.com/.../big/.jpg",
#         "soldout_icon": "",          ← 비어있으면 판매중
#         "basket_display": true,      ← false면 장바구니 불가
#         ...
#       }, ...
#     ]
#   },
#   "is_new_product": true
# }


def _abs_url(path: str) -> str:
    """카페24 상대경로 → 절대 URL 변환."""
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

    종료 조건: rtn_data.data 배열이 비어 있거나 rtn_code != "1000"
    on_page(page, total_so_far): 페이지 처리 완료마다 호출 (UI 진행률용)
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

            # ── JSON 파싱 ────────────────────────────────────────────────
            try:
                payload = _json.loads(body)
            except Exception as e:
                log.error(
                    f"API JSON 파싱 실패 (page={page}): {e}\n"
                    f"응답 앞 500자: {body[:500]}"
                )
                break

            # ── 응답 코드 확인 ────────────────────────────────────────────
            rtn_code = str(payload.get("rtn_code", ""))
            if rtn_code != "1000":
                log.warning(f"API 오류 코드 (page={page}): rtn_code={rtn_code}")
                break

            # ── 상품 리스트 추출: payload["rtn_data"]["data"] ─────────────
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

                href      = _abs_url(p.get("link_product_detail") or "")
                image_url = _abs_url(p.get("image_big") or p.get("image_medium") or "")
                name      = (
                    p.get("product_name_striptag") or
                    p.get("disp_product_name") or ""
                ).strip()
                retail_price = int(p.get("product_price") or 0)

                # 품절 판단: soldout_icon이 있거나 basket_display가 False
                soldout = bool(p.get("soldout_icon")) or not p.get("basket_display", True)

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
    상세 페이지에서 JS 변수를 우선 추출하고, 없으면 DOM 폴백을 사용합니다.

    추출 우선순위:
      1) JS 전역 변수 (product_name, product_image_tiny, option_name_mapper, option_stock_data)
      2) option_array (cafe24 공통 변수)
      3) DOM 셀렉터
    """
    await page.goto(item.url, wait_until="domcontentloaded")
    try:
        await page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass

    js = _as_dict(await page.evaluate(DETAIL_JS))

    name        = js.get("name") or item.name
    image_url   = js.get("image_url") or item.image_url
    base_price  = int(js.get("base_price") or item.retail_price or 0)
    custom_code = str(js.get("custom_code") or item.model or item.wing_code)
    opt1_title  = js.get("opt1_title") or None
    opt2_title  = js.get("opt2_title") or None
    # Python 측 description 정제 (JS 처리의 이중 안전망)
    # ec-data-src → src 치환, base64 인라인 이미지 제거
    _desc_raw = js.get("description") or ""
    _desc_raw = _desc_raw.replace("ec-data-src=", "src=")
    _desc_raw = re.sub(r'\s*src="data:[^"]*"', "", _desc_raw, flags=re.IGNORECASE)
    description = _desc_raw.strip() or name

    # options_raw: JS에서 반환된 배열 — 각 항목을 _as_dict로 정규화
    options_raw = _as_list(js.get("options_raw") or [])

    # ── 옵션 생성 ──────────────────────────────────────────────────────────
    options: list[OptionData] = []
    for raw in options_raw:
        # 신 형식: opt1_name/opt2_name 직접 제공
        # 구 형식: option_value 문자열에서 #$% 분리
        if raw.get("opt1_name") is not None:
            opt1_name    = str(raw.get("opt1_name") or "").strip()
            opt2_name    = str(raw.get("opt2_name") or "").strip()
            retail_price = parse_price(str(raw.get("option_price") or 0))
        else:
            opt_val   = str(raw.get("option_value") or "")
            parts     = opt_val.split("#$%")
            opt1_name = parts[0].strip()
            opt2_name = parts[1].strip() if len(parts) > 1 else ""
            price_add    = parse_price(str(raw.get("option_price") or 0))
            retail_price = base_price + price_add

        stock_raw = str(raw.get("stock_number") or "0").strip()
        stock     = int(stock_raw) if stock_raw.isdigit() else 0

        # 신 형식: is_selling/use_soldout, 구 형식: option_disabled
        if "is_selling" in raw:
            selling     = str(raw.get("is_selling")  or "F").upper() == "T"
            use_soldout = str(raw.get("use_soldout") or "F").upper() == "T"
            disabled    = not selling or use_soldout
        else:
            disabled = str(raw.get("option_disabled") or "F").upper() == "T"

        option_cond = 2 if (stock == 0 or disabled) else 1

        options.append(OptionData(
            opt1_name    = opt1_name or "기본",
            opt2_name    = opt2_name,
            option_cond  = option_cond,
            option_stock = stock,
            cost_price   = round(retail_price * 0.45 * 1.1),
            retail_price = retail_price,
        ))

    # 옵션 없는 단독 구매 상품 — "기본 옵션" 으로 저장
    if not options:
        options.append(OptionData(
            opt1_name    = "기본 옵션",
            opt2_name    = "",
            option_cond  = 2 if item.soldout else 1,
            option_stock = 0,
            cost_price   = round(base_price * 0.45 * 1.1),
            retail_price = base_price,
        ))

    # 모든 옵션이 품절(option_cond==2)이면 상품 전체를 품절로 판단
    all_soldout = all(o.option_cond == 2 for o in options) if options else item.soldout

    product = ProductData(
        wing_code   = item.wing_code,
        name        = name,
        sub_name    = custom_code if custom_code != item.wing_code else None,
        model       = custom_code or item.wing_code,
        product_url = item.url,
        image_url   = image_url,
        description = description,
        opt1_title  = opt1_title,
        opt2_title  = opt2_title,
        soldout     = all_soldout,
    )
    return product, options


# ──────────────────────────────────────────────────────────────────────────────
# Supabase 저장
# ──────────────────────────────────────────────────────────────────────────────

def save_product_and_options(
    product: ProductData,
    options: list[OptionData],
    on_status_change: Optional[Callable[[str], None]] = None,
) -> int:
    """
    변경 감지 후 조건부 upsert 전략:
      - 신규 or 변경 있음 → update_at = 현재시간 갱신
      - 변경 없음        → parsing_at만 갱신, update_at 유지

    비교 항목: prod_cond(품절상태) / option_cond / retail_price / option_stock
    알림 형식: [상태변경] 상품명 - 옵션명: 이전상태 → 현재상태
               [가격변경] 상품명 - 옵션명: 39,000원 → 42,000원
               [재고변경] 상품명 - 옵션명: 5개 → 0개

    on_status_change: 메시지를 전달받는 콜백. None이면 log.info 사용.
    반환: 저장된 옵션 건수
    """
    now = datetime.now(timezone.utc).isoformat()
    _notify: Callable[[str], None] = on_status_change or (lambda msg: log.info(msg))
    new_prod_cond = 2 if product.soldout else 1

    # ── 1. 기존 상품 조회 (id, prod_cond) ────────────────────────────────
    ex_prod_resp = (
        supabase.table("parsing_wing_products")
        .select("id, prod_cond")
        .eq("wing_code", product.wing_code)
        .limit(1)
        .execute()
    )
    ex_prod_rows = _as_list(ex_prod_resp.data) if ex_prod_resp.data else []
    existing_prod  = ex_prod_rows[0] if ex_prod_rows else None
    is_new         = existing_prod is None
    old_prod_id    = None if is_new else existing_prod.get("id")
    old_prod_cond  = 1 if is_new else int(existing_prod.get("prod_cond") or 1)
    prod_cond_changed = (not is_new) and (old_prod_cond != new_prod_cond)

    # ── 2. 기존 옵션 조회 (upsert 전에 비교하기 위해 먼저 실행) ──────────
    # {(opt1_name, opt2_name): {option_cond, retail_price, option_stock}}
    ExOptMap = dict[tuple[str, str], dict]
    existing_opts: ExOptMap = {}
    if old_prod_id:
        ex_opts_resp = (
            supabase.table("parsing_wing_options")
            .select("opt1_name, opt2_name, option_cond, retail_price, option_stock")
            .eq("product_id", old_prod_id)
            .execute()
        )
        for r in _as_list(ex_opts_resp.data if ex_opts_resp.data else []):
            key = (str(r.get("opt1_name") or ""), str(r.get("opt2_name") or ""))
            existing_opts[key] = {
                "option_cond":  int(r.get("option_cond")  or 1),
                "retail_price": int(r.get("retail_price") or 0),
                "option_stock": int(r.get("option_stock") or 0),
            }

    # ── 3. 옵션 변경 사항 수집 (upsert 전에 판단) ────────────────────────
    # 변경이 있어야 update_at을 갱신하므로, 먼저 감지해 둔다.
    opt_changes: list[str] = []   # 알림 메시지 버퍼
    option_data_changed = False

    for o in options:
        key = (o.opt1_name, o.opt2_name)
        ex  = existing_opts.get(key)
        if ex is None:
            continue   # 신규 옵션 — 변경 아닌 추가
        opt_label = o.opt1_name + (f" / {o.opt2_name}" if o.opt2_name else "")

        # 품절 상태
        if ex["option_cond"] != o.option_cond:
            option_data_changed = True
            opt_changes.append(
                f"[상태변경] {product.name} - {opt_label}: "
                f"{_cond_label(ex['option_cond'])} → {_cond_label(o.option_cond)}"
            )

        # 판매가 (0원 방어: 파싱 실패 시 의미 없는 변경 무시)
        if ex["retail_price"] != o.retail_price and o.retail_price > 0:
            option_data_changed = True
            opt_changes.append(
                f"[가격변경] {product.name} - {opt_label}: "
                f"{ex['retail_price']:,}원 → {o.retail_price:,}원"
            )

        # 재고
        if ex["option_stock"] != o.option_stock:
            option_data_changed = True
            opt_changes.append(
                f"[재고변경] {product.name} - {opt_label}: "
                f"{ex['option_stock']}개 → {o.option_stock}개"
            )

    # ── 4. update_at 조건부 결정 + prev_retail_price 계산 ───────────────
    # 신규 or prod_cond 변경 or 옵션 데이터 변경 → update_at 갱신
    # 아무 변경도 없으면 → parsing_at만 갱신, update_at은 row에서 제외(기존값 유지)
    any_changed = is_new or prod_cond_changed or option_data_changed

    # 가격 변경이 있을 때 이전 최소 판매가를 prev_retail_price로 기록
    # (supabase_schema.sql의 ALTER TABLE 명령으로 컬럼을 먼저 추가해야 합니다)
    prev_retail_price: Optional[int] = None
    if option_data_changed and existing_opts:
        old_prices = [
            v["retail_price"]
            for v in existing_opts.values()
            if v["retail_price"] > 0
        ]
        if old_prices:
            prev_retail_price = min(old_prices)

    # ── 5. 상품 upsert (supabase.ts 실존 컬럼만, brand/model_name 제외) ──
    prod_row: dict = {
        "wing_code":   product.wing_code,
        "name":        product.name,
        "sub_name":    product.sub_name,
        "model":       product.model,
        "product_url": product.product_url,
        "image_url":   product.image_url,
        "description": product.description,
        "opt1_title":  product.opt1_title,
        "opt2_title":  product.opt2_title,
        "regi_cond":   1,
        "prod_cond":   new_prod_cond,
        "parsing_at":  now,
        # update_at: 변경이 있을 때만 포함 → ON CONFLICT SET에 포함 여부 결정
    }
    if any_changed:
        prod_row["update_at"] = now
    if prev_retail_price is not None:
        prod_row["prev_retail_price"] = prev_retail_price

    resp = (
        supabase.table("parsing_wing_products")
        .upsert(prod_row, on_conflict="wing_code")
        .execute()
    )
    rows = _as_list(resp.data) if resp.data else []
    if not rows:
        raise RuntimeError(f"상품 upsert 응답 없음 (wing_code={product.wing_code})")

    product_id: int = rows[0].get("id")
    if not product_id:
        raise RuntimeError(f"upsert 응답에 id 없음 (wing_code={product.wing_code}, data={rows[0]})")

    # ── 6. 변경 알림 출력 ────────────────────────────────────────────────
    if prod_cond_changed:
        _notify(
            f"[상태변경] {product.name}: "
            f"{_cond_label(old_prod_cond)} → {_cond_label(new_prod_cond)}"
        )
    for msg in opt_changes:
        _notify(msg)

    # ── 7. 기존 옵션 전체 삭제 후 최신 데이터 재삽입 ─────────────────────
    supabase.table("parsing_wing_options").delete().eq("product_id", product_id).execute()

    # parsing_wing_options Insert — supabase.ts 실존 컬럼만 (option_name 없음)
    opt_rows = [
        {
            "product_id":   product_id,
            "opt1_name":    o.opt1_name,
            "opt2_name":    o.opt2_name,
            "option_cond":  o.option_cond,
            "option_stock": o.option_stock,
            "cost_price":   o.cost_price,
            "retail_price": o.retail_price,
            "changed_at":   now,
        }
        for o in options
    ]
    if opt_rows:
        supabase.table("parsing_wing_options").insert(opt_rows).execute()

    return len(opt_rows)


def update_soldout_status(wing_code: str, soldout: bool) -> None:
    """품절 상태만 빠르게 업데이트 (상세 페이지 방문 없음)."""
    supabase.table("parsing_wing_products").update({
        "prod_cond": 2 if soldout else 1,
        "update_at": datetime.now(timezone.utc).isoformat(),
    }).eq("wing_code", wing_code).execute()


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

async def run(max_pages: int = 9999) -> None:
    """
    CLI 진입점.

    Phase 1: API 루프로 전체 상품 wing_code 수집 (Playwright 브라우저 불필요)
    Phase 2: 상세 페이지 방문 → JS 변수 추출 → DB upsert
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    started   = datetime.now(timezone.utc)
    total = success = fail = opt_total = 0

    async with async_playwright() as pw:
        ctx    = await create_browser_context(pw)
        det_pg = await ctx.new_page()

        # ── Phase 1: API 루프로 전체 목록 수집 ────────────────────────────
        log.info("Phase 1 — API 루프 목록 수집 시작")
        all_listings = await fetch_all_listings_via_api(ctx, max_pages=max_pages)
        log.info(f"Phase 1 완료 — 총 {len(all_listings)}건")

        # ── Phase 2: 상세 파싱 + 저장 ────────────────────────────────────
        log.info("Phase 2 — 상품 상세 파싱 + 저장")
        for item in all_listings:
            total += 1
            last_err: Optional[Exception] = None
            for attempt in range(1, 3):
                try:
                    product, options = await scrape_product_detail(det_pg, item)
                    saved = save_product_and_options(product, options)
                    opt_total += saved
                    success   += 1
                    min_price = min(
                        (o.retail_price for o in options if o.retail_price > 0),
                        default=0,
                    )
                    log.info(
                        f"  [수집완료] {product.name} "
                        f"(가격: {min_price:,}원, 상태: {_cond_label(2 if product.soldout else 1)}, "
                        f"옵션: {saved}건)"
                    )
                    last_err = None
                    break
                except Exception as exc:
                    last_err = exc
                    if attempt < 2:
                        await asyncio.sleep(2)
            if last_err:
                fail += 1
                log.warning(f"  NG [{item.wing_code}] {friendly_db_error(last_err)}")

        await ctx.close()

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print_summary(total, success, fail, opt_total, elapsed)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="윙하우스 상품 파싱")
    ap.add_argument("--pages", type=int, default=1, help="파싱할 페이지 수 (기본: 1)")
    args = ap.parse_args()
    asyncio.run(run(max_pages=args.pages))
