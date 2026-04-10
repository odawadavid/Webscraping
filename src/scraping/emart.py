from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
from pathlib import Path
from datetime import date

baskets = ['milk', 'sugar', 'bread', 'rice', 'cooking oil', 'wheat flour', 'maize flour']

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_PATH = BASE_DIR / 'data' / 'raw' / 'emart.csv'


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
            ]
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

        # Hide webdriver fingerprint
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        page = context.new_page()

        for item in baskets:
            print(f"\n🔍 Searching for: {item}")

            # ── Go directly to search URL ──
            query = item.replace(" ", "+")
            url = f"https://e-mart.co.ke/index.php?category_id=0&search={query}&submit_search=&route=product%2Fsearch"

            try:
                page.goto(url, wait_until="networkidle", timeout=60000)
            except PlaywrightTimeoutError:
                print(f"  ⚠️  Page load timed out for '{item}', trying domcontentloaded...")
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_timeout(3000)
                except Exception as e:
                    print(f"  ❌ Failed to load page for '{item}': {e}")
                    continue

            # ── Wait for product containers ──
            product_selector = ".product-layout"

            try:
                page.wait_for_selector(product_selector, timeout=20000)
            except PlaywrightTimeoutError:
                print(f"  ⚠️  No products found for '{item}'")
                page.screenshot(path=f"debug_{item.replace(' ', '_')}.png")
                continue

            # ── Scroll to trigger lazy loading ──
            print(f"  📜 Scrolling to load more products...")
            for _ in range(5):
                page.mouse.wheel(0, 3000)
                page.wait_for_timeout(1000)

            products = page.locator(product_selector).all()
            print(f"  🃏 {len(products)} products found")

            seen_links = set()

            for product in products:
                try:
                    # ── Name ──
                    name_el = product.locator("h4 a").first
                    if name_el.count() == 0:
                        continue
                    name = name_el.inner_text().strip()
                    if len(name) < 3:
                        continue

                    # ── Current price ──
                    current_price = "n/a"
                    price_el = product.locator("span.price-new").first
                    if price_el.count() > 0:
                        current_price = price_el.inner_text().strip()

                    # ── Old price (if on sale) ──
                    old_price = "No discount"
                    old_price_el = product.locator("span.price-old").first
                    if old_price_el.count() > 0:
                        old_price = old_price_el.inner_text().strip()

                    # ── Link ──
                    link = ""
                    link_el = product.locator("h4 a").first
                    if link_el.count() > 0:
                        link = link_el.get_attribute("href") or ""
                    if link and not link.startswith("http"):
                        link = "https://e-mart.co.ke" + link

                    if not link or link in seen_links:
                        continue
                    seen_links.add(link)

                    all_products.append({
                        "date":          date.today().isoformat(),
                        "search":        item,
                        "name":          name[:70],
                        "current_price": current_price,
                        "old_price":     old_price,
                        "link":          link,
                        "source":        "E-Mart",
                    })

                    discount = f"  (was {old_price})" if old_price != "No discount" else ""
                    print(f"    ✔ {name[:50]:<50} {current_price}{discount}")

                except Exception as e:
                    print(f"    ⚠️  Skipped a product: {e}")

        browser.close()

    # ── Save to CSV ────────────────────────────────────────────────────────
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(all_products)
    
    if len(df) == 0:
        print(f"⚠️  No products scraped for E-Mart")
    else:
        df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
        print(f"\n✅ Done! Total products collected: {len(df)}")
        print(f"💾  Saved to {OUTPUT_PATH}")

    return df


if __name__ == "__main__":
    run_emart()