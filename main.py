import os
import json
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page
from supabase import create_client, Client

load_dotenv()

CONFIG_PATH = "config.json"

SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ──────────────────────────────────────────────
# 설정 로더
# ──────────────────────────────────────────────

def load_config(path: str = CONFIG_PATH) -> dict:
    """config.json을 읽어 파이썬 dict로 반환."""
    with open(path, encoding="utf-8") as f:
        config = json.load(f)
    return config


# ──────────────────────────────────────────────
# 결과 집계용 데이터 클래스
# ──────────────────────────────────────────────

@dataclass
class ScrapeResult:
    site_name:   str
    scraped:     int = 0
    inserted:    int = 0
    updated:     int = 0
    unchanged:   int = 0
    errors:      list[str] = field(default_factory=list)
    started_at:  datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None

    @property
    def elapsed_sec(self) -> float:
        end = self.finished_at or datetime.now(timezone.utc)
        return (end - self.started_at).total_seconds()


# ──────────────────────────────────────────────
# 스크래핑
# ──────────────────────────────────────────────

async def scrape_products(page: Page, site: dict) -> list[dict]:
    """한 사이트의 상품 목록을 파싱해 [{name, price, url, scraped_at}] 반환."""
    sel = site["selectors"]
    base_url = site["url"]

    await page.goto(base_url, wait_until="domcontentloaded")
    await page.wait_for_selector(sel["item"], timeout=15_000)

    items = await page.query_selector_all(sel["item"])
    products: list[dict] = []

    for item in items:
        name_el  = await item.query_selector(sel["name"])
        price_el = await item.query_selector(sel["price"])
        link_el  = await item.query_selector(sel.get("link", "a"))

        name  = (await name_el.inner_text()).strip()  if name_el  else None
        price = (await price_el.inner_text()).strip() if price_el else None
        href  = await link_el.get_attribute("href")  if link_el  else None

        if href:
            product_url = (
                href if href.startswith("http")
                else f"{base_url.rstrip('/')}/{href.lstrip('/')}"
            )
        else:
            product_url = None

        if name:
            products.append({
                "name":       name,
                "price":      price,
                "url":        product_url,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })

    return products


# ──────────────────────────────────────────────
# Supabase 저장 + 변경 건수 집계
# ──────────────────────────────────────────────

def save_to_supabase(products: list[dict], site: dict, result: ScrapeResult) -> None:
    """
    상품 목록을 upsert하고, 신규/변경/미변경 건수를 result에 기록한다.

    판별 방법:
      - DB에 url 없음  → inserted
      - DB에 있고 price 달라짐 → updated
      - DB에 있고 price 동일  → unchanged
    """
    if not products:
        result.errors.append("수집된 상품이 없습니다.")
        return

    db_cfg   = site["supabase"]
    table    = db_cfg["table"]
    conflict = db_cfg["conflict_column"]

    # ① 기존 레코드 조회 (url, price만)
    scraped_urls = [p["url"] for p in products if p["url"]]
    existing_rows: dict[str, str | None] = {}

    if scraped_urls:
        rows = (
            supabase.table(table)
            .select("url, price")
            .in_("url", scraped_urls)
            .execute()
        )
        existing_rows = {row["url"]: row["price"] for row in (rows.data or [])}

    # ② 신규/변경/미변경 분류
    for p in products:
        url = p.get("url")
        if url not in existing_rows:
            result.inserted += 1
        elif existing_rows[url] != p["price"]:
            result.updated += 1
        else:
            result.unchanged += 1

    # ③ upsert
    supabase.table(table).upsert(products, on_conflict=conflict).execute()


# ──────────────────────────────────────────────
# 터미널 요약 출력
# ──────────────────────────────────────────────

def print_summary(results: list[ScrapeResult]) -> None:
    """모든 사이트의 크롤링 결과를 표 형태로 요약 출력."""
    sep = "─" * 52

    print(f"\n{sep}")
    print("  크롤링 완료 요약")
    print(sep)
    print(f"  {'사이트':<18} {'수집':>4} {'신규':>4} {'변경':>4} {'유지':>4} {'소요':>6}")
    print(sep)

    total_scraped   = 0
    total_inserted  = 0
    total_updated   = 0
    total_unchanged = 0

    for r in results:
        status = " [오류]" if r.errors else ""
        print(
            f"  {r.site_name:<18} "
            f"{r.scraped:>4}건 "
            f"{r.inserted:>4}건 "
            f"{r.updated:>4}건 "
            f"{r.unchanged:>4}건 "
            f"{r.elapsed_sec:>5.1f}s"
            f"{status}"
        )
        for err in r.errors:
            print(f"    ⚠ {err}")

        total_scraped   += r.scraped
        total_inserted  += r.inserted
        total_updated   += r.updated
        total_unchanged += r.unchanged

    if len(results) > 1:
        print(sep)
        print(
            f"  {'합계':<18} "
            f"{total_scraped:>4}건 "
            f"{total_inserted:>4}건 "
            f"{total_updated:>4}건 "
            f"{total_unchanged:>4}건"
        )

    print(sep)
    print(f"  실행 시각: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{sep}\n")


# ──────────────────────────────────────────────
# 진입점
# ──────────────────────────────────────────────

async def main() -> None:
    config  = load_config()
    browser_cfg = config["browser"]
    active_sites = [s for s in config["sites"] if s.get("enabled", True)]

    if not active_sites:
        print("활성화된 사이트가 없습니다. config.json의 enabled 값을 확인하세요.")
        return

    results: list[ScrapeResult] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=browser_cfg["headless"])
        context = await browser.new_context(user_agent=browser_cfg["user_agent"])

        for site in active_sites:
            result = ScrapeResult(site_name=site["name"])
            print(f"\n[{site['name']}] 스크래핑 시작: {site['url']}")

            try:
                page     = await context.new_page()
                products = await scrape_products(page, site)
                await page.close()

                result.scraped = len(products)
                print(f"[{site['name']}] 수집 완료: {result.scraped}건 → Supabase 저장 중...")

                save_to_supabase(products, site, result)

            except Exception as exc:
                result.errors.append(str(exc))
                print(f"[{site['name']}] 오류 발생: {exc}")

            finally:
                result.finished_at = datetime.now(timezone.utc)
                results.append(result)

        await browser.close()

    print_summary(results)


if __name__ == "__main__":
    asyncio.run(main())
