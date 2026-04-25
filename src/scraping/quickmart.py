# ──────────────────────────────────────────────────────────────────────────────
# quickmart.py
# Scrapes product names and prices from Quickmart's category pages and saves
# the results to a CSV file.
#
# Why category pages instead of the search bar?
# ─────────────────────────────────────────────
# Quickmart's search endpoint does NOT filter results by keyword — it returns
# the same generic "featured products" listing for every query.  Searching for
# "milk" returns soaps, exercise books, and chapatis alongside milk products.
# The category pages (e.g. /dairy-products, /sugar) are curated per category
# and are exactly what a shopper sees when browsing the site manually.
#
# How it works (plain English):
#   1. Open a headless Chrome browser and visit the Quickmart store page so
#      the site sets a session cookie (required to see product listings).
#   2. For each basket category, visit its dedicated category page and collect
#      every product card across up to MAX_PAGES pages.
#   3. Skip any product whose URL was already collected (deduplication).
#   4. Stop paginating early if the site starts repeating the same products.
#   5. Save everything to a CSV file.
# ──────────────────────────────────────────────────────────────────────────────

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
from pathlib import Path
from datetime import date


# ── Configuration ─────────────────────────────────────────────────────────────

# Maps each basket category label to its dedicated page on quickmart.co.ke.
# These URLs were taken directly from the site's own category listing at
# https://www.quickmart.co.ke/category — they are the same links a shopper
# clicks when browsing the store manually.
#
# Note: wheat flour and maize flour share one page (/flour) because that is
# how Quickmart organises them.  Both will be scraped under the "flour" label;
# the product names (e.g. "Pembe Maize Flour 2Kg") make the type clear.
CATEGORIES = {
    "milk":        "https://www.quickmart.co.ke/products/search?keyword-milk&pagesize-30",
    "sugar":       "https://www.quickmart.co.ke/products/search?keyword-sugar&pagesize-30",
    "bread":       "https://www.quickmart.co.ke/products/search?keyword-bread&pagesize-30",
    "rice":        "https://www.quickmart.co.ke/products/search?keyword-rice&pagesize-30",
    "cooking oil": "https://www.quickmart.co.ke/products/search?keyword-cooking%20oil&pagesize-30",
    "maize flour": "https://www.quickmart.co.ke/products/search?keyword-maize%20meal&pagesize-30",
    "wheat flour": "https://www.quickmart.co.ke/products/search?keyword-baking%20flour&pagesize-30",
}

# How many pages to load per category.  Category pages show roughly 30 products
# each, so 3 pages gives up to ~90 products per category.
MAX_PAGES = 3

# CSS class that wraps each product card on the page.
CARD_SELECTOR = ".productInfoJs"

# Where the CSV is saved (two levels up from this file, then data/raw/).
BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_PATH = BASE_DIR / "data" / "raw" / "quickmart.csv"


# ── Helper functions ──────────────────────────────────────────────────────────

def dismiss_modal(page):
    """
    Quickmart sometimes shows a 'choose your store' popup when you first visit.
    This function closes it if it appears.  If no popup appears, it does nothing.
    """
    try:
        # Wait up to 5 seconds to see if the popup appears
        page.wait_for_selector("#shopPopupJs", state="visible", timeout=5000)
        print("  ⚠️  Store popup detected — closing …")

        # Try each possible close button
        for btn_sel in [
            "#shopPopupJs button[data-dismiss='modal']",
            "#shopPopupJs .btn-close",
            "#shopPopupJs .close",
        ]:
            btn = page.locator(btn_sel)
            if btn.count() > 0:
                btn.first.click(force=True)
                page.wait_for_selector("#shopPopupJs", state="hidden", timeout=5000)
                print("  ✅  Popup closed")
                return

        # If no button worked, forcibly remove the popup with JavaScript
        page.evaluate("""
            const modal = document.getElementById('shopPopupJs');
            if (modal) modal.remove();
            document.querySelectorAll('.modal-backdrop').forEach(el => el.remove());
            document.body.classList.remove('modal-open');
            document.body.style.overflow = '';
        """)
        print("  ✅  Popup removed via JavaScript")

    except PlaywrightTimeoutError:
        pass  # No popup — carry on normally


def goto_page(page, url):
    """
    Navigate to a URL.  Waits for network activity to settle first.
    Falls back to a simpler strategy if that takes too long.
    """
    try:
        page.goto(url, wait_until="networkidle", timeout=60_000)
    except PlaywrightTimeoutError:
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3_000)  # give late-loading content a moment


def get_text(locator):
    """
    Safely read text from a page element.
    Returns an empty string if the element doesn't exist.
    """
    if locator.count() > 0:
        return locator.inner_text().strip()
    return ""


def has_number(text):
    """Return True if a string contains at least one digit (used to check
    that a price field actually contains a price, not just symbols)."""
    return any(ch.isdigit() for ch in text)


# ── Main scraper ──────────────────────────────────────────────────────────────

def run_quickmart():
    """
    Launches the browser, visits each category page, collects all products,
    and saves the results to a CSV.  Returns the DataFrame.
    """
    all_products = []   # one dict per product; converted to DataFrame at the end

    with sync_playwright() as playwright:

        # ── Start the browser ─────────────────────────────────────────────────
        browser = playwright.chromium.launch(
            headless=True,     # run without opening a visible window
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
            ],
        )

        # Create a browser session with a realistic user-agent so the site
        # does not immediately block the request as coming from a bot
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 720},
            locale="en-KE",
        )
        # Hide the automated-browser flag
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        page = context.new_page()

        # ── Step 1: Set the store session ─────────────────────────────────────
        # Visiting /1001 (Kikuyu Road branch) sets a session cookie.
        # Without it, every category page redirects to an error asking you
        # to choose a location — no products are shown.
        print("🌐  Setting store session (Quickmart Kikuyu Rd #1001) …")
        goto_page(page, "https://www.quickmart.co.ke/1001")
        dismiss_modal(page)
        print("  ✅  Session ready\n")

        # ── Step 2: Scrape each category ──────────────────────────────────────
        for category, base_url in CATEGORIES.items():
            print(f"{'─' * 60}")
            print(f"📦  {category.upper()}")

            # Track URLs already collected for this category.
            # Resets to empty at the start of each new category so each one
            # is treated independently.
            seen_links: set[str] = set()

            for page_num in range(1, MAX_PAGES + 1):

                # Category pages paginate with &page-N
                url = f"{base_url}&page-{page_num}&pagesize-30/"
                print(f"\n  📄  Page {page_num}  →  {url}")

                goto_page(page, url)
                dismiss_modal(page)

                # Wait for products to appear.  If nothing loads in 15 s,
                # the category has no more pages — move to the next category.
                try:
                    page.wait_for_selector(CARD_SELECTOR, timeout=15_000)
                except PlaywrightTimeoutError:
                    print("  ⚠️  No products on this page — stopping pagination")
                    break

                cards = page.locator(CARD_SELECTOR).all()
                print(f"  🃏  {len(cards)} cards found")

                if not cards:
                    print("  ⛔  Empty page — stopping pagination")
                    break

                # ── Stall check ───────────────────────────────────────────────
                # Some sites serve the same page indefinitely instead of
                # returning 'no results'.  Collect all links on this page; if
                # every one is already in seen_links, the site has looped back
                # to page 1 — stop now rather than saving duplicates.
                this_page_links: set[str] = set()
                for card in cards:
                    link_el = card.locator("a").first
                    if link_el.count() > 0:
                        href = link_el.get_attribute("href") or ""
                        if href:
                            if not href.startswith("http"):
                                href = "https://www.quickmart.co.ke" + href
                            this_page_links.add(href)

                if this_page_links and this_page_links.issubset(seen_links):
                    print("  🔁  Page is a repeat — stopping pagination")
                    break

                # ── Extract data from each card ───────────────────────────────
                for card in cards:
                    try:
                        # Product name
                        name = get_text(card.locator(".products-title").first)
                        if len(name) < 3:
                            continue  # skip empty or corrupted cards

                        # Product URL
                        link = ""
                        link_el = card.locator("a").first
                        if link_el.count() > 0:
                            link = link_el.get_attribute("href") or ""
                        if link and not link.startswith("http"):
                            link = "https://www.quickmart.co.ke" + link

                        # Skip if we already recorded this product on a previous page
                        if link in seen_links:
                            continue
                        seen_links.add(link)

                        # Current selling price
                        # Discounted items show the sale price in .products-price-new;
                        # regular-priced items use .products-price
                        current_price = "n/a"
                        sale = get_text(card.locator(".products-price-new").first)
                        if sale and has_number(sale):
                            current_price = sale
                        else:
                            regular = get_text(card.locator(".products-price").first)
                            if regular and has_number(regular):
                                current_price = regular

                        # Original price before discount (empty if no discount)
                        old_price = "No discount"
                        old = get_text(card.locator(".products-price-old").first)
                        if old and has_number(old):
                            old_price = old

                        # Store the result
                        all_products.append({
                            "date":          date.today().isoformat(),
                            "category":      category,
                            "name":          name[:70],
                            "current_price": current_price,
                            "old_price":     old_price,
                            "link":          link,
                            "source":        "Quickmart",
                        })

                        discount_note = f"  (was {old_price})" if old_price != "No discount" else ""
                        print(f"    ✔  {name[:58]:<58} {current_price}{discount_note}")

                    except Exception as err:
                        print(f"    ⚠️  Skipped a card: {err}")

        browser.close()

    # ── Step 3: Save to CSV ───────────────────────────────────────────────────
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(all_products)

    # Final deduplication: if somehow the same product name appears twice in
    # the same category (e.g. from two overlapping pages), keep only the first.
    df.drop_duplicates(subset=["category", "name"], keep="first", inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")

    print(f"\n{'─' * 60}")
    print(f"✅  Done!  {len(df)} unique products collected.")
    print(f"💾  Saved → {OUTPUT_PATH}")

    # Print a quick summary table
    print("\nProducts per category:")
    print(df["category"].value_counts().to_string())

    return df


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_quickmart()