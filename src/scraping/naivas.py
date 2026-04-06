from playwright.sync_api import sync_playwright
import pandas as pd
from pathlib import Path

baskets = ['milk', 'sugar', 'bread', 'cooking oil', 'wheat flour', 'maize flour']

def run_naivas():

    BASE_DIR = Path(__file__).resolve().parents[2]
    file_path = BASE_DIR / 'data' / 'raw' / 'naivas.csv'
    
    all_products = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, slow_mo=500)
        page = browser.new_page()

        page.goto('https://naivas.online/')
        page.wait_for_selector("input[type='search']", timeout=30000)

        for item in baskets:
            print(f"\n🔍 Searching for: {item}")

            search_box = page.locator("input[type='search']").first
            search_box.click()
            search_box.fill("")
            search_box.fill(item)
            page.wait_for_timeout(1000)
            search_box.press("Enter")

            try:
                page.wait_for_url("**/search**", timeout=10000)
            except:
                pass

            product_link_selector = "a.\\!text-naivas-gray-dark"

            try:
                page.wait_for_selector(product_link_selector, timeout=10000)
            except:
                print(f"  ⚠️  No products found for '{item}'.")
                print(f"       Current URL  : {page.url}")
                print(f"       Total <a> tags: {page.locator('a[href]').count()}")
                continue

            # ── Scroll down 5 times to trigger lazy loading ───────────────
            print(f"  📜 Scrolling to load more products...")

            for _ in range(15):
                page.mouse.wheel(0, 4000)
                page.wait_for_timeout(2000)
            cards = page.locator(product_link_selector).all()
            print(f"  🃏 {len(cards)} cards found")

            seen_links = set()

            for card in cards:
                try:
                    # ── Product name ───────────────────────────────────────
                    name_el = card.locator("span.line-clamp-2").first
                    name = name_el.inner_text().strip() if name_el.count() > 0 else ""

                    # ── FIX: The real HTML tree from the inspector ─────────
                    #
                    # <div class="flex flex-col grow">        ← 2 levels up from <a>
                    #   <div class="text-black-50 ...">       ← 1 level up from <a>
                    #       <a class="!text-naivas-gray-dark"> ← our card
                    #   <div class="mb-4 grow">               ← SIBLING (not ancestor!)
                    #       <div class="product-price">
                    #           <span class="text-naivas-green">KES 57</span>
                    #           <span class="line-through">KES 61</span>
                    #
                    # mb-4 is a sibling of the name div, not a parent.
                    # XPath ancestor:: only walks UP — it can never find siblings.
                    # We must go up to the shared parent "flex flex-col grow"
                    # which is exactly 2 levels above the <a> tag.
                    # From there we can search DOWN into both branches.

                    parent = card.locator("xpath=../..").first  # 2 levels up → div.flex.flex-col.grow

                    # ── Current price (green bold span) ────────────────────
                    # From the HTML: <span class="font-bold text-naivas-green mb-1 md:mb-0 pe-2">
                    current_price_el = parent.locator("span.text-naivas-green").first
                    current_price = current_price_el.inner_text().strip() if current_price_el.count() > 0 else "—"

                    # ── Old price (red strikethrough span) ─────────────────
                    # From the HTML: <span class="text-red-600 text-xs line-through font-light">
                    old_price_el = parent.locator("span.text-red-600.text-xs.line-through.font-light").first
                    old_price = old_price_el.inner_text().strip() if old_price_el.count() > 0 else "No discount"

                    # ── Full product link ──────────────────────────────────
                    link = card.get_attribute("href") or ""
                    if link and not link.startswith("http"):
                        link = "https://naivas.online" + link

                    if not name or len(name) < 3 or link in seen_links:
                        continue
                    seen_links.add(link)

                    all_products.append({
                        "search":        item,
                        "name":          name[:70],
                        "current_price": current_price,
                        "old_price":     old_price,
                        "link":          link
                    })

                    discount = f"  (was {old_price})" if old_price != "No discount" else ""
                    print(f"    ✔ {name[:50]:<50} {current_price}{discount}")

                except Exception as e:
                    print(f"    ⚠️  Skipped a card: {e}")

        browser.close()

    # ── Save to CSV using pandas ───────────────────────────────────────────
    output_file = file_path
    df = pd.DataFrame(all_products)
    df.to_csv(output_file, index=False, encoding="utf-8")

    print(f"\n✅ Done! Total products collected: {len(df)}")
    print(f"💾  Saved to {output_file}")
    print(f"\n📊 Preview:\n")
    #print(df[["name", "current_price", "old_price"]].to_string(index=False))
    print('\n\nFile successfully saved at:\n', file_path)

    return df


if __name__ == "__main__":
    run_naivas()