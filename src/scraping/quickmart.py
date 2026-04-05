from playwright.sync_api import sync_playwright
import pandas as pd
from pathlib import Path

baskets = ['milk', 'sugar', 'bread', 'cooking oil', 'wheat flour', 'maize flour']


def run_quickmart():

    BASE_DIR = Path(__file__).resolve().parents[2]
    file_path = BASE_DIR / 'data' / 'raw' / 'quickmart.csv'
    
    all_products = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False, slow_mo=500)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()

        # ── 1. Go directly to a store – bypasses the location modal entirely ──
        print("🌐  Opening Quickmart store #1501 …")
        page.goto("https://www.quickmart.co.ke/1501", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)

        # ── 2. Dismiss any residual modal just in case ─────────────────────────
        print("📍 Checking for any residual modal …")
        try:
            page.wait_for_selector("#locationInfoBox", timeout=4000)
            print("  ⚠️  Modal still appeared – trying location input …")

            location_input = page.locator("#location_fld")
            location_input.wait_for(state="visible", timeout=4000)
            location_input.fill("Kinoo Mama Ngina Stage, Rungiri, Kenya")
            page.wait_for_timeout(2000)

            try:
                page.wait_for_selector(
                    ".pac-item, ul.pac-container li, [class*='suggestion']",
                    timeout=3000
                )
                print("  ✅ Autocomplete suggestions visible")
            except Exception:
                print("  ⚠️  No suggestions appeared")

            page.keyboard.press("ArrowDown")
            page.wait_for_timeout(500)
            page.keyboard.press("Enter")
            page.wait_for_timeout(1500)

            # If modal still open, force close it
            try:
                page.wait_for_selector("#locationInfoBox", timeout=2000)
                print("  ⚠️  Modal still open – force closing …")
                for btn_sel in [
                    "#locationInfoBox button[data-dismiss='modal']",
                    "#locationInfoBox .btn-close",
                    "#locationInfoBox .close",
                ]:
                    btn = page.locator(btn_sel)
                    if btn.count() > 0:
                        btn.first.click(force=True)
                        page.wait_for_timeout(600)
                        print(f"  ✅ Closed via {btn_sel}")
                        break
                else:
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(600)
                    print("  ✅ Closed via Escape")
            except Exception:
                print("  ✅ Modal closed after location entry")

        except Exception:
            print("  ✅ No modal detected – proceeding\n")

        # ── 3. Wait for search box ────────────────────────────────────────────
        print("🔎  Locating search box …")
        page.wait_for_selector("input[placeholder*='search' i]", timeout=10000)
        search_box = page.locator("input[placeholder*='search' i]").first
        print("  ✅ Search box ready\n")

        # ── 4. Dump the page structure once to confirm product card selector ──
        print("🕵️  Sampling page to detect product card selector …")
        search_box.click()
        search_box.fill("milk")
        page.wait_for_timeout(800)
        search_box.press("Enter")
        page.wait_for_timeout(3000)

        # Print counts for all candidate selectors so we can see what matches
        candidates = [
            "li.product",
            "li.type-product",
            ".product-card",
            ".product-inner",
            ".product-wrapper",
            ".woocommerce-loop-product__link",
            "ul.products li",
            "div.products div",
            "[class*='product']:not(script):not(style)",
        ]
        print("  Selector probe results:")
        best_sel = None
        for sel in candidates:
            try:
                count = page.locator(sel).count()
                if count > 0:
                    # Peek at first match's inner text to confirm it's a product
                    sample = page.locator(sel).first.inner_text()[:60].replace("\n", " ")
                    print(f"    {sel:<50} → {count:>3} matches  |  sample: {sample!r}")
                    if best_sel is None and count >= 2:
                        best_sel = sel
            except Exception as e:
                print(f"    {sel:<50} → error: {e}")

        if best_sel is None:
            print("\n  ❌ Could not auto-detect selector. Saving debug HTML …")
            with open("debug_probe.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            browser.close()
            return pd.DataFrame()

        print(f"\n  ✅ Using card selector: '{best_sel}'\n")

        # ── 5. Now search for all basket items ────────────────────────────────
        for item in baskets:
            print(f"🔍 Searching for: {item}")

            # Dismiss any reappearing modal
            try:
                page.wait_for_selector("#locationInfoBox", timeout=2000)
                page.keyboard.press("Escape")
                page.wait_for_timeout(500)
                print("  ⚠️  Modal dismissed before search")
            except Exception:
                pass

            search_box.click()
            search_box.fill("")
            search_box.fill(item)
            page.wait_for_timeout(800)
            search_box.press("Enter")

            try:
                page.wait_for_url("**/search**", timeout=8000)
            except Exception:
                pass
            page.wait_for_timeout(1500)

            # Dismiss modal if it appeared on results page
            try:
                page.wait_for_selector("#locationInfoBox", timeout=2000)
                page.keyboard.press("Escape")
                page.wait_for_timeout(500)
            except Exception:
                pass

            # Confirm cards exist
            try:
                page.wait_for_selector(best_sel, timeout=8000)
            except Exception:
                print(f"  ⚠️  No results for '{item}' — saving debug HTML")
                with open(f"debug_{item.replace(' ', '_')}.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                continue

            MAX_PAGES = 5   # safety limit

            for page_num in range(1, MAX_PAGES + 1):
            
                search_url = f"https://www.quickmart.co.ke/products/search?keyword-{item}&page-{page_num}"
            
                print(f"  📄 Loading page {page_num}")
            
                page.goto(search_url, wait_until="domcontentloaded")
            
                page.wait_for_timeout(2000)
            
                
                """cards = page.locator(best_sel).all()
            
                if len(cards) == 0:
                    print("  ⛔ No more products — stopping pagination")
                    break
            
                print(f"  🃏 {len(cards)} products found")
            
                for card in cards:
                    # existing extraction logic here
                    pass"""

            cards = page.locator(best_sel).all()
            print(f"  🃏 {len(cards)} cards found")

            if not cards:
                break

            seen_links: set = set()

            for card in cards:
                try:
                    # ── Name ──────────────────────────────────────────────────
                    name = ""
                    for name_sel in [
                        "h2.woocommerce-loop-product__title",
                        ".woocommerce-loop-product__title",
                        ".product-title", ".product-name",
                        "h2", "h3",
                        "[class*='title']",
                    ]:
                        el = card.locator(name_sel).first
                        if el.count() > 0:
                            txt = el.inner_text().strip()
                            if len(txt) >= 3:
                                name = txt
                                break

                    if len(name) < 3:
                        continue

                    # Reject store-name false positives
                    if "quickmart" in name.lower() and len(name) < 25:
                        continue

                    # ── Current price ─────────────────────────────────────────
                    current_price = "n/a"
                    for price_sel in [
                        "ins .woocommerce-Price-amount",
                        ".woocommerce-Price-amount",
                        ".price .amount",
                        ".price",
                        "[class*='price']",
                    ]:
                        el = card.locator(price_sel).first
                        if el.count() > 0:
                            txt = el.inner_text().strip()
                            if txt and any(c.isdigit() for c in txt):
                                current_price = txt
                                break

                    # ── Old price ─────────────────────────────────────────────
                    old_price = "No discount"
                    for old_sel in [
                        "del .woocommerce-Price-amount",
                        ".line-through",
                        "del",
                        "s",
                        "[class*='old']",
                        "[class*='original']",
                    ]:
                        el = card.locator(old_sel).first
                        if el.count() > 0:
                            txt = el.inner_text().strip()
                            if txt and any(c.isdigit() for c in txt):
                                old_price = txt
                                break

                    # ── Link ──────────────────────────────────────────────────
                    link = ""
                    for link_sel in [
                        "a.woocommerce-loop-product__link",
                        "a.product-link",
                        "a",
                    ]:
                        el = card.locator(link_sel).first
                        if el.count() > 0:
                            link = el.get_attribute("href") or ""
                            if link:
                                break

                    if link and not link.startswith("http"):
                        link = "https://www.quickmart.co.ke" + link

                    if link in seen_links:
                        continue
                    seen_links.add(link)

                    all_products.append({
                        "search":        item,
                        "name":          name[:70],
                        "current_price": current_price,
                        "old_price":     old_price,
                        "link":          link,
                    })

                    disc = f"  (was {old_price})" if old_price != "No discount" else ""
                    print(f"    ✔ {name[:50]:<50} {current_price}{disc}")

                except Exception as e:
                    print(f"    ⚠️  Skipped card: {e}")

        browser.close()

    # ── 6. Save to CSV ────────────────────────────────────────────────────────
    output_file = file_path
    df = pd.DataFrame(all_products)
    df.to_csv(output_file, index=False, encoding="utf-8")

    print(f"\n✅ Done! {len(df)} products collected.")
    print(f"💾  Saved → {output_file}")
    if not df.empty:
        print(f"\n📊 Preview:\n")
        #print(df[["search", "name", "current_price", "old_price"]].to_string(index=False))
    else:
        print("\n⚠️  0 products — check the debug HTML files for correct selectors.")

    return df


if __name__ == "__main__":
    run_quickmart()