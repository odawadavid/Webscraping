from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
from pathlib import Path
from datetime import date

baskets = ['milk', 'sugar', 'bread', 'rice', 'cooking oil', 'wheat flour', 'maize flour']

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_PATH = BASE_DIR / 'data' / 'raw' / 'naivas.csv'

def run_naivas():
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

            # ── Go directly to search results URL — no search box needed ──
            query = item.replace(" ", "+")
            url = f"https://www.naivas.online/search?term={query}"

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

            # ── Dismiss cookie banner if present ──────────────────────────
            try:
                page.click("button[aria-label='Accept all cookies']", timeout=5000)
                page.wait_for_load_state("networkidle", timeout=10000)
                print("  🍪 Cookie banner dismissed")
            except PlaywrightTimeoutError:
                pass  # No banner, continue

            # ── Wait for product cards ─────────────────────────────────────
            product_link_selector = "a.\\!text-naivas-gray-dark"

            try:
                page.wait_for_selector(product_link_selector, timeout=20000)
            except PlaywrightTimeoutError:
                print(f"  ⚠️  No products found for '{item}'")
                print(f"       URL: {page.url}")
                # Save debug screenshot in CI
                page.screenshot(path=f"debug_{item.replace(' ', '_')}.png")
                continue

            # ── Scroll to trigger lazy loading ────────────────────────────
            print(f"  📜 Scrolling to load more products...")
            for _ in range(15):
                page.mouse.wheel(0, 4000)
                page.wait_for_timeout(1500)

            cards = page.locator(product_link_selector).all()
            print(f"  🃏 {len(cards)} cards found")

            seen_links = set()

            for card in cards:
                try:
                    name_el = card.locator("span.line-clamp-2").first
                    name = name_el.inner_text().strip() if name_el.count() > 0 else ""

                    parent = card.locator("xpath=../..").first

                    current_price_el = parent.locator("span.text-naivas-green").first
                    current_price = (
                        current_price_el.inner_text().strip()
                        if current_price_el.count() > 0 else "—"
                    )

                    old_price_el = parent.locator(
                        "span.text-red-600.text-xs.line-through.font-light"
                    ).first
                    old_price = (
                        old_price_el.inner_text().strip()
                        if old_price_el.count() > 0 else "No discount"
                    )

                    link = card.get_attribute("href") or ""
                    if link and not link.startswith("http"):
                        link = "https://www.naivas.online" + link

                    if not name or len(name) < 3 or link in seen_links:
                        continue
                    seen_links.add(link)

                    all_products.append({
                        "date":          date.today().isoformat(),
                        "search":        item,
                        "name":          name[:70],
                        "current_price": current_price,
                        "old_price":     old_price,
                        "link":          link,
                        "source":        "Naivas",
                    })

                    discount = f"  (was {old_price})" if old_price != "No discount" else ""
                    print(f"    ✔ {name[:50]:<50} {current_price}{discount}")

                except Exception as e:
                    print(f"    ⚠️  Skipped a card: {e}")

        browser.close()

    # ── Save to CSV ────────────────────────────────────────────────────────
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(all_products)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")

    print(f"\n✅ Done! Total products collected: {len(df)}")
    print(f"💾  Saved to {OUTPUT_PATH}")

    return df


if __name__ == "__main__":
    run_naivas()