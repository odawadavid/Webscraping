from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
from pathlib import Path
from datetime import date

baskets = ['milk', 'sugar', 'bread', 'rice', 'cooking oil', 'wheat flour', 'maize flour']

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_PATH = BASE_DIR / 'data' / 'raw' / 'emart.csv'

MAX_PAGES = 8  # Safety cap — increase if needed


def build_url(query: str, page: int) -> str:
    """Return the canonical search URL for a given query and page number."""
    encoded = query.replace(" ", "+")
    if page == 1:
        return (
            f"https://e-mart.co.ke/index.php"
            f"?route=product/search&search={encoded}&category_id=0"
        )
    return (
        f"https://e-mart.co.ke/index.php"
        f"?route=product/search&search={encoded}&category_id=0&page={page}"
    )


def scrape_page(page, item: str, page_num: int, seen_links: set) -> list:
    """
    Navigate to one search-results page and extract all products on it.
    Returns a list of product dicts. Returns an empty list when:
      - the page fails to load
      - no product containers are found (signals end of results)
    """
    url = build_url(item, page_num)
    print(f"    🌐 Page {page_num}: {url}")

    # ── Load the page ──────────────────────────────────────────────────────
    try:
        page.goto(url, wait_until="networkidle", timeout=60_000)
    except PlaywrightTimeoutError:
        print(f"    ⚠️  networkidle timed out — retrying with domcontentloaded…")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(3_000)
        except Exception as exc:
            print(f"    ❌ Failed to load page {page_num} for '{item}': {exc}")
            return []

    # ── Wait for product containers ────────────────────────────────────────
    product_selector = ".product-layout"
    try:
        page.wait_for_selector(product_selector, timeout=20_000)
    except PlaywrightTimeoutError:
        # No products → we have exhausted all pages for this query
        print(f"    ℹ️  No products on page {page_num} — stopping pagination.")
        return []

    # ── Scroll to trigger lazy-loading ────────────────────────────────────
    for _ in range(5):
        page.mouse.wheel(0, 3_000)
        page.wait_for_timeout(1_000)

    products = page.locator(product_selector).all()
    print(f"    🃏 {len(products)} product card(s) found")

    records = []
    for product in products:
        try:
            # Name ─────────────────────────────────────────────────────────
            name_el = product.locator("h4 a").first
            if name_el.count() == 0:
                continue
            name = name_el.inner_text().strip()
            if len(name) < 3:
                continue

            # Current price ────────────────────────────────────────────────
            current_price = "n/a"
            price_el = product.locator("span.price-new").first
            if price_el.count() > 0:
                current_price = price_el.inner_text().strip()

            # Old / pre-discount price ─────────────────────────────────────
            old_price = "No discount"
            old_price_el = product.locator("span.price-old").first
            if old_price_el.count() > 0:
                old_price = old_price_el.inner_text().strip()

            # Link ─────────────────────────────────────────────────────────
            link_el = product.locator("h4 a").first
            link = link_el.get_attribute("href") or "" if link_el.count() > 0 else ""
            if link and not link.startswith("http"):
                link = "https://e-mart.co.ke" + link

            # Skip duplicates across pages ─────────────────────────────────
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            records.append({
                "date":          date.today().isoformat(),
                "search":        item,
                "name":          name[:70],
                "current_price": current_price,
                "old_price":     old_price,
                "link":          link,
                "source":        "E-Mart",
            })

            discount = f"  (was {old_price})" if old_price != "No discount" else ""
            print(f"      ✔ {name[:50]:<50} {current_price}{discount}")

        except Exception as exc:
            print(f"      ⚠️  Skipped a product card: {exc}")

    return records


def run_emart():
    all_products = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
            ],
        )

        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 720},
            locale="en-KE",
        )

        # Mask automation fingerprint
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        page = context.new_page()

        for item in baskets:
            print(f"\n🔍 Searching for: {item}")
            seen_links: set = set()
            item_total = 0

            for page_num in range(1, MAX_PAGES + 1):
                records = scrape_page(page, item, page_num, seen_links)

                if not records:
                    # Empty page → no more results for this query
                    break

                all_products.extend(records)
                item_total += len(records)

                # If fewer cards than a typical full page are returned we are
                # likely on the last page — no need to fetch one more empty page.
                if len(records) < 10:
                    print(f"    ℹ️  Partial page ({len(records)} items) — assuming last page.")
                    break
            else:
                print(f"  ⚠️  Reached MAX_PAGES ({MAX_PAGES}) for '{item}'.")

            print(f"  📦 '{item}' total collected: {item_total}")

        browser.close()

    # ── Save to CSV ────────────────────────────────────────────────────────
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(all_products)

    if df.empty:
        print("\n⚠️  No products scraped for E-Mart.")
    else:
        df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
        print(f"\n✅ Done! Total products collected: {len(df)}")
        print(f"💾  Saved to {OUTPUT_PATH}")

    return df


if __name__ == "__main__":
    run_emart()