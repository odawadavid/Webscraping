import pandas as pd
import re
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path

from brands import KNOWN_BRANDS, COMMODITY_CATEGORIES

pd.set_option('display.max_colwidth', None)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

CATEGORIES = {
    r'sugar':                          'Sugar',
    r'bread':                          'Bread',
    r'milk|uht':                       'Milk',
    r'rice':                           'Rice',
    r'cooking oil|vegetable oil|oil':  'Cooking Oil',
    r'wheat|flour|baking':             'Wheat Flour',
    r'maize meal|maize flour|ugali':   'Maize Meal',
}

COLUMN_ORDER = [
    'name', 'category', 'brand', 'sku', 'store',
    'base_size', 'size', 'unit_type', 'unit_value',
    'old_price', 'current_price', 'discount',
    'price_per_unit', 'date', 'link',
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def extract_store(url: str) -> str | None:
    """Derive a clean store name from a product URL."""
    try:
        domain = urlparse(url).netloc
        if 'naivas'    in domain: return 'Naivas'
        if 'quickmart' in domain: return 'Quickmart'
        if 'e-mart'    in domain or 'emart' in domain: return 'E-Mart'
    except Exception:
        pass
    return None


def clean_price_column(col: pd.Series) -> pd.Series:
    """Strip currency symbols and noise, return a float Series."""
    return (
        col.astype(str)
           .str.replace('KES',         '', regex=False)
           .str.replace('\xa0',        '', regex=False)
           .str.replace(',',           '', regex=False)
           .str.replace('\n',          '', regex=False)
           .str.replace('No discount', '0', regex=False)
           .str.replace('n/a',         '0', regex=False)
           .str.strip()
           .str.findall(r'\d+\.?\d*')
           .str[0]
           .astype(float)
    )


def extract_brand(name: str, category: str) -> str:
    """
    Return a canonical brand name for a product.

    Strategy
    ────────
    1. For commodity categories (Sugar, Rice, Flour …) return 'Generic'.
       These categories use a brand-free SKU — see build_sku().

    2. For branded categories (Milk …) scan KNOWN_BRANDS for a regex match.
       Returns the canonical brand name if found.

    3. Fallback: first capitalised word of the name.
       This is imperfect but logged so you can add missing brands to brands.py.
    """
    if category in COMMODITY_CATEGORIES:
        return 'Generic'

    patterns = KNOWN_BRANDS.get(category, [])
    for pattern, canonical in patterns:
        if re.search(pattern, name, flags=re.IGNORECASE):
            return canonical

    # Fallback — first word; log so it can be added to brands.py later
    first_word = name.capitalize().split()[0] if name.strip() else 'Unknown'
    print(f"    ⚠️  Unknown brand in '{category}': \"{name[:60]}\" → defaulting to \"{first_word}\"")
    return first_word


def build_sku(category: str, brand: str, base_size: str) -> str:
    """
    Two-tier SKU construction.

    Commodity categories  →  category + base_size
      e.g.  "Sugar1kg"
      Rationale: enables cross-store comparison of "cheapest 1kg sugar"
      regardless of which brand each store stocks.

    Branded categories    →  category + brand + base_size
      e.g.  "MilkBrookside500ml"
      Rationale: Brookside 500ml and Daima 500ml are different products
      worth tracking separately.
    """
    if category in COMMODITY_CATEGORIES:
        return f"{category}{base_size}"
    return f"{category}{brand}{base_size}"


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_cleaning() -> pd.DataFrame:
    BASE_DIR      = Path(__file__).resolve().parents[2]
    RAW_DIR       = BASE_DIR / 'data' / 'raw'
    PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
    HISTORY_DIR   = BASE_DIR / 'data' / 'history'

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    # ── 1. Load raw CSVs ──────────────────────────────────────────────────────
    dfs = []
    for store in ['naivas', 'quickmart', 'emart']:
        path = RAW_DIR / f'{store}.csv'
        if not path.exists() or path.stat().st_size == 0:
            print(f"  ⚠️  {store}.csv is missing or empty — skipping")
            continue
        try:
            dfs.append(pd.read_csv(path))
        except pd.errors.EmptyDataError:
            print(f"  ⚠️  {store}.csv has no parseable data — skipping")

    if not dfs:
        print("  ❌ No data from any store — aborting")
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # ── 2. Name ───────────────────────────────────────────────────────────────
    df['name'] = df['name'].str.lower().str.strip()

    # ── 3. Store ──────────────────────────────────────────────────────────────
    df['store'] = df['link'].apply(extract_store)

    bad_store = df['store'].isna()
    if bad_store.any():
        print(f"  ⚠️  {bad_store.sum()} rows dropped — store could not be inferred from link")
        df = df[~bad_store].copy()

    # ── 4. Base size ──────────────────────────────────────────────────────────
    df['base_size'] = (
        df['name']
          .str.extract(r'(\d+\s?(?:ml|l|ltr|g|kg))', flags=re.IGNORECASE)[0]
          .str.lower()
          .str.replace(' ', '', regex=False)
    )

    # ── 5. Category ───────────────────────────────────────────────────────────
    df['category'] = 'Other'
    for pattern, label in CATEGORIES.items():
        mask = df['name'].str.contains(pattern, regex=True, na=False)
        df.loc[mask, 'category'] = label

    df = df[df['category'] != 'Other'].copy()

    # ── 6. Drop helper columns ────────────────────────────────────────────────
    for col in ['search', 'source']:
        if col in df.columns:
            df = df.drop(columns=[col])

    # ── 7. Prices ─────────────────────────────────────────────────────────────
    df['current_price'] = clean_price_column(df['current_price'])
    df['old_price']     = clean_price_column(df['old_price']).fillna(0)

    no_price = df['current_price'].isna() | (df['current_price'] == 0)
    if no_price.any():
        print(f"  ⚠️  {no_price.sum()} rows dropped — zero / missing current price")
        df = df[~no_price].copy()

    # ── 8. Base size — final filter ───────────────────────────────────────────
    bad_size = (
        df['base_size'].isna() |
        (df['base_size'] == '') |
        (df['base_size'] == '06800')
    )
    if bad_size.any():
        print(f"  ⚠️  {bad_size.sum()} rows dropped — invalid base_size")
        df = df[~bad_size].copy()

    # ── 9. Size (numeric) ─────────────────────────────────────────────────────
    df['size'] = (
        df['base_size']
          .str.replace(r'(?:ml|l|ltr|g|kg)', '', regex=True)
          .astype('float64')
    )

    # ── 10. Unit type & unit value ────────────────────────────────────────────
    df = df.reset_index(drop=True)
    df['unit_type']  = None
    df['unit_value'] = None

    has_kg = df['base_size'].str.contains('kg',  case=False, na=False)
    has_g  = df['base_size'].str.contains('g',   case=False, na=False) & ~has_kg
    has_ml = df['base_size'].str.contains('ml',  case=False, na=False)
    has_l  = df['base_size'].str.contains('l',   case=False, na=False) & ~has_ml

    df.loc[has_kg, 'unit_type']  = 'kg'
    df.loc[has_g,  'unit_type']  = 'kg'
    df.loc[has_ml, 'unit_type']  = 'litres'
    df.loc[has_l,  'unit_type']  = 'litres'

    df.loc[has_kg, 'unit_value'] = df.loc[has_kg, 'size']
    df.loc[has_g,  'unit_value'] = df.loc[has_g,  'size'] / 1000
    df.loc[has_ml, 'unit_value'] = df.loc[has_ml, 'size'] / 1000
    df.loc[has_l,  'unit_value'] = df.loc[has_l,  'size']

    df['unit_value'] = df['unit_value'].astype('float64')

    bad_unit = df['unit_value'].isna() | (df['unit_value'] == 0)
    if bad_unit.any():
        print(f"  ⚠️  {bad_unit.sum()} rows dropped — could not compute unit_value")
        df = df[~bad_unit].copy()

    # ── 11. Derived columns ───────────────────────────────────────────────────
    df['discount']       = (df['old_price'] - df['current_price']).where(df['old_price'] > 0, 0)
    df['price_per_unit'] = df['current_price'] / df['unit_value']
    df['date']           = datetime.now().strftime('%Y-%m-%d')

    # ── 12. Brand & SKU (two-tier) ────────────────────────────────────────────
    df['brand'] = df.apply(
        lambda row: extract_brand(row['name'], row['category']), axis=1
    )
    df['sku'] = df.apply(
        lambda row: build_sku(row['category'], row['brand'], row['base_size']), axis=1
    )

    # ── 13. Final column order ────────────────────────────────────────────────
    df = df[COLUMN_ORDER].reset_index(drop=True)

    # ── 14. Save latest snapshot ──────────────────────────────────────────────
    latest_path = PROCESSED_DIR / 'latest_items.csv'
    df.to_csv(latest_path, index=False)
    print(f"  💾 Latest snapshot saved ({len(df)} rows) → {latest_path}")

    # ── 15. Smart-delta history append ────────────────────────────────────────
    history_path = HISTORY_DIR / 'price_history.csv'

    try:
        old = pd.read_csv(history_path, dtype={'date': str})

        last_known = (
            old.sort_values('date')
               .drop_duplicates(subset=['sku', 'store'], keep='last')
               [['sku', 'store', 'price_per_unit']]
               .rename(columns={'price_per_unit': 'price_per_unit_last'})
        )

        merged = df.merge(last_known, on=['sku', 'store'], how='left')

        changed_mask = (
            merged['price_per_unit_last'].isna() |
            (merged['price_per_unit'].round(4) != merged['price_per_unit_last'].round(4))
        )
        new_rows = merged[changed_mask].drop(columns=['price_per_unit_last'])

        if new_rows.empty:
            print("  ℹ️  No price changes detected today — history unchanged")
        else:
            print(f"  📈 {len(new_rows)} price change(s) detected across "
                  f"{new_rows['store'].nunique()} store(s)")
            updated_history = (
                pd.concat([old, new_rows], ignore_index=True)
                  .drop_duplicates(subset=['sku', 'store', 'date'])
                  .sort_values(['sku', 'store', 'date'])
            )
            updated_history.to_csv(history_path, index=False)
            print(f"  💾 Price history updated ({len(updated_history)} total rows) → {history_path}")

    except FileNotFoundError:
        df.to_csv(history_path, index=False)
        print(f"  💾 Price history created ({len(df)} rows) → {history_path}")

    print('✅ Cleaning completed successfully.')
    return df


if __name__ == '__main__':
    run_cleaning()