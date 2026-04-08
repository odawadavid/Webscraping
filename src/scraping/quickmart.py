from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
from pathlib import Path
from datetime import date

baskets = ['milk', 'sugar', 'bread', 'rice', 'cooking oil', 'wheat flour', 'maize flour']

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_PATH = BASE_DIR / 'data' / 'raw' / 'quickmart.csv'

MAX_PAGES = 5
CARD_SELECTOR = ".productInfoJs"


def dismiss_modal(page):
    try:
        page.wait_for_selector("#shopPopupJs", state="visible", timeout=5000)
        print("  ⚠️  Store modal detected – dismissing …")
        for btn_sel in [
            "#shopPopupJs button[data-dismiss='modal']",
            "#shopPopupJs .btn-close",
            "#shopPopupJs .close",
        ]:
            btn = page.locator(btn_sel)
            if btn.count() > 0:
                btn.first.click(force=True)
                page.wait_for_selector("#shopPopupJs", state="hidden", timeout=5000)
                print(f"  ✅ Modal closed via {btn_sel}")
                return
        page.evaluate("""
            const modal = document.getElementById('shopPopupJs');
            if (modal) modal.remove();
            document.querySelectorAll('.modal-backdrop').forEach(el => el.remove());
            document.body.classList.remove('modal-open');
            document.body.style.overflow = '';
        """)
        print("  ✅ Modal removed via JS")
    except PlaywrightTimeoutError:
        print("  ✅ No modal – proceeding")


def run_quickmart():
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
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = context.new_page()

        # ── 1. Open store to set session/cookies ──────────────────────────────
        print("🌐  Opening Quickmart store #1501 …")
        try:
            page.goto(
                "https://www.quickmart.co.ke/1501",
                wait_until="networkidle",
                timeout=60000
            )
        except PlaywrightTimeoutError:
            page.goto(
                "https://www.quickmart.co.ke/1501",
                wait_until="domcontentloaded",
                timeout=60000
            )
            page.wait_for_timeout(3000)

        dismiss_modal(page)

        # ── 2. Scrape each basket item ─────────────────────────────────────────
        for item in baskets:
            print(f"\n🔍 Searching for: {item}")
            query = item.replace(" ", "-")

            for page_num in range(1, MAX_PAGES + 1):
                search_url = (
                    f"https://www.quickmart.co.ke/products/search"
                    f"?keyword={query}&page={page_num}"
                )
                print(f"  📄 Page {page_num} → {search_url}")

                try:
                    page.goto(search_url, wait_until="networkidle", timeout=60000)
                except PlaywrightTimeoutError:
                    page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_timeout(3000)

                dismiss_modal(page)

                # Wait for product cards
                try:
                    page.wait_for_selector(CARD_SELECTOR, timeout=15000)
                except PlaywrightTimeoutError:
                    print(f"  ⚠️  No products on page {page_num} — stopping")
                    page.screenshot(
                        path=f"debug_quickmart_{item.replace(' ', '_')}_p{page_num}.png"
                    )
                    break

                cards = page.locator(CARD_SELECTOR).all()
                print(f"  🃏 {len(cards)} cards found")

                if not cards:
                    print("  ⛔ Empty page — stopping pagination")
                    break

                seen_links: set = set()

                for card in cards:
                    try:
                        # ── Name ──────────────────────────────────────────────
                        name_el = card.locator(".products-title").first
                        if name_el.count() == 0:
                            continue
                        name = name_el.inner_text().strip()
                        if len(name) < 3:
                            continue

                        # ── Current price ──────────────────────────────────────
                        current_price = "n/a"
                        price_el = card.locator(".products-price-new").first
                        if price_el.count() > 0:
                            txt = price_el.inner_text().strip()
                            if txt and any(c.isdigit() for c in txt):
                                current_price = txt

                        # Fallback: if no sale price, use the regular price
                        if current_price == "n/a":
                            fallback_el = card.locator(".products-price").first
                            if fallback_el.count() > 0:
                                txt = fallback_el.inner_text().strip()
                                if txt and any(c.isdigit() for c in txt):
                                    current_price = txt

                        # ── Old price ──────────────────────────────────────────
                        old_price = "No discount"
                        old_el = card.locator(".products-price-old").first
                        if old_el.count() > 0:
                            txt = old_el.inner_text().strip()
                            if txt and any(c.isdigit() for c in txt):
                                old_price = txt

                        # ── Link ───────────────────────────────────────────────
                        link = ""
                        link_el = card.locator("a").first
                        if link_el.count() > 0:
                            link = link_el.get_attribute("href") or ""
                        if link and not link.startswith("http"):
                            link = "https://www.quickmart.co.ke" + link

                        if link in seen_links:
                            continue
                        seen_links.add(link)

                        all_products.append({
                            "date":          date.today().isoformat(),
                            "search":        item,
                            "name":          name[:70],
                            "current_price": current_price,
                            "old_price":     old_price,
                            "link":          link,
                            "source":        "Quickmart",
                        })

                        disc = f"  (was {old_price})" if old_price != "No discount" else ""
                        print(f"    ✔ {name[:50]:<50} {current_price}{disc}")

                    except Exception as e:
                        print(f"    ⚠️  Skipped card: {e}")

        browser.close()

    # ── 3. Save to CSV ────────────────────────────────────────────────────────
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(all_products)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")

    print(f"\n✅ Done! {len(df)} products collected.")
    print(f"💾  Saved → {OUTPUT_PATH}")

    return df


if __name__ == "__main__":
    run_quickmart()