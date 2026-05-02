"""
emart.py — Production scraper for e-mart.co.ke
================================================
Fixes vs original
-----------------
* Catches ALL Playwright errors on page.goto (not just PlaywrightTimeoutError).
  net::ERR_CONNECTION_TIMED_OUT is a generic playwright Error, not a timeout.
* Per-item isolation: one basket item failing never kills the rest.
* Retry loop with exponential back-off + jitter on every page load.
* Pipeline always saves partial results even if some items fail completely.
* Stealth browser args tuned for GitHub Actions (no-sandbox, single-process, etc.).
* Random inter-request delay to avoid rate-limiting on CI/cloud IPs.
* Graceful empty-DataFrame handling so downstream code never receives None.
"""

from __future__ import annotations

import random
import time
import traceback
from datetime import date
from pathlib import Path

import pandas as pd
from playwright.sync_api import (
    Error as PlaywrightError,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

# ── Configuration ──────────────────────────────────────────────────────────────

BASKETS: list[str] = [
    "milk",
    "sugar",
    "bread",
    "rice",
    "cooking oil",
    "wheat flour",
    "maize flour",
]

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_PATH = BASE_DIR / "data" / "raw" / "emart.csv"

MAX_PAGES          = 8      # hard cap per search term
PAGE_TIMEOUT_MS    = 90_000  # 90 s — longer for slow CI connections
SELECTOR_TIMEOUT   = 25_000  # 25 s wait for product cards
MAX_RETRIES        = 3      # attempts per page before giving up
BACKOFF_BASE_S     = 5      # seconds; doubles each retry
REQUEST_DELAY_S    = (2, 5)  # random pause between pages (min, max)
SCROLL_STEPS       = 5      # lazy-load scroll iterations


# ── URL builder ───────────────────────────────────────────────────────────────

def build_url(query: str, page_num: int) -> str:
    """Return the canonical search URL for a given query and page number."""
    encoded = query.replace(" ", "+")
    base = (
        f"https://e-mart.co.ke/index.php"
        f"?route=product/search&search={encoded}&category_id=0"
    )
    return base if page_num == 1 else f"{base}&page={page_num}"


# ── Page loader with full retry logic ────────────────────────────────────────

def _load_page(page, url: str) -> bool:
    """
    Attempt to navigate to *url* up to MAX_RETRIES times.

    Strategy per attempt:
      1. Try `networkidle` (best fidelity).
      2. On PlaywrightTimeoutError fall back to `domcontentloaded` + 3 s sleep.
      3. On any other PlaywrightError (includes ERR_CONNECTION_TIMED_OUT)
         wait with exponential back-off then retry from scratch.

    Returns True on success, False when all retries are exhausted.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT_MS)
            return True

        except PlaywrightTimeoutError:
            # Site loaded but JS never went quiet — DOM is probably fine.
            print(f"      ⚠️  networkidle timeout (attempt {attempt}) — "
                  "retrying with domcontentloaded…")
            try:
                page.goto(
                    url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS
                )
                page.wait_for_timeout(3_000)
                return True
            except PlaywrightError as inner:
                print(f"      ⚠️  domcontentloaded also failed: {inner}")
                # Fall through to back-off below

        except PlaywrightError as exc:
            # Covers net::ERR_CONNECTION_TIMED_OUT, ERR_NAME_NOT_RESOLVED, etc.
            print(f"      ⚠️  Connection error (attempt {attempt}/{MAX_RETRIES}): {exc}")

        # Exponential back-off before next attempt
        if attempt < MAX_RETRIES:
            sleep_s = BACKOFF_BASE_S * (2 ** (attempt - 1)) + random.uniform(0, 2)
            print(f"      ⏳  Waiting {sleep_s:.1f}s before retry…")
            time.sleep(sleep_s)

    return False


# ── Single-page scraper ───────────────────────────────────────────────────────

def scrape_page(page, item: str, page_num: int, seen_links: set) -> list[dict]:
    """
    Navigate to one search-results page and extract all products.

    Returns a list of product dicts.
    Returns an empty list when:
      - the page fails to load after all retries
      - no product containers are found (end of pagination)
    """
    url = build_url(item, page_num)
    print(f"    🌐 Page {page_num}: {url}")

    # ── Load ──────────────────────────────────────────────────────────────────
    if not _load_page(page, url):
        print(f"    ❌ Failed to load page {page_num} for '{item}' "
              f"after {MAX_RETRIES} attempts — skipping.")
        return []

    # ── Wait for product cards ─────────────────────────────────────────────
    product_selector = ".product-layout"
    try:
        page.wait_for_selector(product_selector, timeout=SELECTOR_TIMEOUT)
    except PlaywrightTimeoutError:
        print(f"    ℹ️  No products on page {page_num} — stopping pagination.")
        return []

    # ── Lazy-load scroll ──────────────────────────────────────────────────
    for _ in range(SCROLL_STEPS):
        page.mouse.wheel(0, 3_000)
        page.wait_for_timeout(800)

    products = page.locator(product_selector).all()
    print(f"    🃏 {len(products)} product card(s) found")

    # ── Extract data ──────────────────────────────────────────────────────
    records: list[dict] = []
    for product in products:
        try:
            # Name
            name_el = product.locator("h4 a").first
            if name_el.count() == 0:
                continue
            name = name_el.inner_text().strip()
            if len(name) < 3:
                continue

            # Current price
            current_price = "n/a"
            price_el = product.locator("span.price-new").first
            if price_el.count() > 0:
                current_price = price_el.inner_text().strip()

            # Old / pre-discount price
            old_price = "No discount"
            old_price_el = product.locator("span.price-old").first
            if old_price_el.count() > 0:
                old_price = old_price_el.inner_text().strip()

            # Link
            link_el = product.locator("h4 a").first
            link = (link_el.get_attribute("href") or "") if link_el.count() > 0 else ""
            if link and not link.startswith("http"):
                link = "https://e-mart.co.ke" + link

            # Dedup
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            records.append(
                {
                    "date":          date.today().isoformat(),
                    "search":        item,
                    "name":          name[:70],
                    "current_price": current_price,
                    "old_price":     old_price,
                    "link":          link,
                    "source":        "E-Mart",
                }
            )

            discount = f"  (was {old_price})" if old_price != "No discount" else ""
            print(f"      ✔ {name[:50]:<50} {current_price}{discount}")

        except Exception as exc:  # noqa: BLE001
            print(f"      ⚠️  Skipped a product card: {exc}")

    return records


# ── Per-item scraper (isolated so one bad item doesn't kill others) ───────────

def scrape_item(page, item: str) -> list[dict]:
    """
    Scrape all pages for a single basket item.
    Any unhandled exception is caught here — the caller always receives a list.
    """
    print(f"\n🔍 Searching for: {item}")
    seen_links: set[str] = set()
    all_records: list[dict] = []

    try:
        for page_num in range(1, MAX_PAGES + 1):
            records = scrape_page(page, item, page_num, seen_links)

            if not records:
                break  # end of results or load failure

            all_records.extend(records)

            if len(records) < 10:
                print(f"    ℹ️  Partial page ({len(records)} items) — last page.")
                break

            # Polite delay between pages
            delay = random.uniform(*REQUEST_DELAY_S)
            print(f"    ⏳  Pausing {delay:.1f}s…")
            time.sleep(delay)
        else:
            print(f"  ⚠️  Reached MAX_PAGES ({MAX_PAGES}) for '{item}'.")

    except Exception:  # noqa: BLE001
        print(f"  ❌ Unexpected error while scraping '{item}':")
        traceback.print_exc()

    print(f"  📦 '{item}' total collected: {len(all_records)}")
    return all_records


# ── Main entry point ──────────────────────────────────────────────────────────

def run_emart() -> pd.DataFrame:
    """
    Scrape all basket items and write results to CSV.
    Always returns a DataFrame (empty if nothing was scraped).
    Never raises — all errors are handled internally.
    """
    all_products: list[dict] = []

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-gpu",
                    "--disable-setuid-sandbox",
                    "--single-process",           # more stable on GitHub Actions
                    "--no-zygote",
                    "--disable-extensions",
                ],
            )

            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
                locale="en-KE",
                # Randomise timezone to look less like a bot
                timezone_id="Africa/Nairobi",
            )

            # Mask webdriver flag
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            page = context.new_page()

            for item in BASKETS:
                records = scrape_item(page, item)
                all_products.extend(records)

                # Inter-item delay
                if item != BASKETS[-1]:
                    delay = random.uniform(*REQUEST_DELAY_S)
                    print(f"\n  ⏳  Inter-item pause {delay:.1f}s…")
                    time.sleep(delay)

            browser.close()

    except Exception:  # noqa: BLE001
        print("❌ Fatal browser error:")
        traceback.print_exc()

    # ── Persist ───────────────────────────────────────────────────────────────
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(all_products)

    if df.empty:
        print("\n⚠️  No products scraped for E-Mart — writing empty CSV.")
        # Write an empty file so downstream jobs don't fail on a missing artifact
        pd.DataFrame(
            columns=["date", "search", "name", "current_price",
                     "old_price", "link", "source"]
        ).to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    else:
        df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
        print(f"\n✅ Done! Total products collected: {len(df)}")
        print(f"💾  Saved to {OUTPUT_PATH}")

    return df


if __name__ == "__main__":
    run_emart()
