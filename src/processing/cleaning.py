import pandas as pd, re
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path

pd.set_option('display.max_colwidth', None)

def run_cleaning():
    BASE_DIR = Path(__file__).resolve().parents[2]

    RAW_DIR       = BASE_DIR / 'data' / 'raw'
    PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
    HISTORY_DIR   = BASE_DIR / 'data' / 'history'

    # Ensure output dirs exist
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    # ── Load data safely ──────────────────────────────────────────────────────
    dfs = []
    for store in ['naivas', 'quickmart', 'emart']:
        path = RAW_DIR / f'{store}.csv'
        if not path.exists() or path.stat().st_size == 0:
            print(f"  ⚠️  {store}.csv is missing or empty — skipping")
            continue
        try:
            dfs.append(pd.read_csv(path))
        except pd.errors.EmptyDataError:
            print(f"  ⚠️  {store}.csv has no data — skipping")

    if not dfs:
        print("  ❌ No data from any store — nothing to clean")
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # ── Clean 'name' column ───────────────────────────────────────────────────
    df['name'] = df['name'].str.lower().str.strip()

    # ── Create 'brand' column ─────────────────────────────────────────────────
    df['brand'] = df['name'].str.capitalize().str.split().str[0]

    # ── Create 'store' column ─────────────────────────────────────────────────
    def extract_store(url):
        try:
            domain = urlparse(url).netloc
            if 'naivas' in domain:
                return 'Naivas'
            elif 'quickmart' in domain:
                return 'Quickmart'
            elif 'e-mart' in domain or 'emart' in domain:
                return 'E-Mart'
        except:
            return None
    df['store'] = df['link'].apply(extract_store)

    # ── Create 'base_size' column ─────────────────────────────────────────────
    df['base_size'] = df['name'].str.extract(
        r'(\d+\s?(?:ml|l|ltr|g|kg))',
        flags=re.IGNORECASE
    )
    df['base_size'] = df['base_size'].str.lower().str.replace(' ', '', regex=False)

    # ── Create 'category' column ──────────────────────────────────────────────
    categories = {
        r'sugar':                          'Sugar',
        r'bread':                          'Bread',
        r'milk|uht':                       'Milk',
        r'rice':                           'Rice',
        r'cooking oil|vegetable oil|oil':  'Cooking Oil',
        r'wheat|flour|baking':             'Wheat Flour',
        r'maize meal|maize flour|ugali':   'Maize Meal',
    }
    df['category'] = 'Other'
    for pattern, value in categories.items():
        df.loc[df['name'].str.contains(pattern, regex=True, na=False), 'category'] = value

    df = df[df['category'] != 'Other']
    if 'search' in df.columns:
        df = df.drop(columns=['search'])

    # ── Clean prices ──────────────────────────────────────────────────────────
    def clean_prices(col):
        return (
            col.astype(str)
               .str.replace('KES ', '',    regex=False)
               .str.replace('\xa0', '',    regex=False)
               .str.replace('KES', '',     regex=False)
               .str.replace(',', '',       regex=False)
               .str.replace('\n', '',      regex=False)
               .str.replace('No discount', '0', regex=False)
               .str.replace('n/a', '0',   regex=False)
               .str.strip()
        )

    df[['current_price', 'old_price']] = df[['current_price', 'old_price']].apply(clean_prices)

    df['current_price'] = df['current_price'].str.findall(r'\d+\.?\d*').str[0].astype(float)
    df['old_price']     = df['old_price'].str.findall(r'\d+\.?\d*').str[0].astype(float)
    df['old_price']     = df['old_price'].fillna(0)

    # ── Clean 'base_size' ─────────────────────────────────────────────────────
    df = df[(df['base_size'] != '') & (df['base_size'] != '06800') & df['base_size'].notna()]

    # ── Create 'size' column ──────────────────────────────────────────────────
    df['size'] = df['base_size'].str.replace(r'(?:ml|l|ltr|g|kg)', '', regex=True).astype('float64')

    # ── Create 'unit_type' column ─────────────────────────────────────────────
    df['unit_type'] = None
    df.loc[df['base_size'].str.contains('kg',  case=False, na=False), 'unit_type'] = 'kg'
    df.loc[df['base_size'].str.contains('g')  & ~df['base_size'].str.contains('kg'), 'unit_type'] = 'kg'
    df.loc[df['base_size'].str.contains('ml',  case=False, na=False), 'unit_type'] = 'litres'
    df.loc[df['base_size'].str.contains('l')  & ~df['base_size'].str.contains('ml'), 'unit_type'] = 'litres'

    # ── Create 'discount' column ──────────────────────────────────────────────
    df['discount'] = (df['old_price'] - df['current_price']).where(df['old_price'] > 0, 0)

    # ── Create 'unit_value' column ────────────────────────────────────────────
    df = df.reset_index(drop=True)
    df['unit_value'] = None
    df.loc[df['base_size'].str.contains('kg',  case=False, na=False), 'unit_value'] = df['size']
    df.loc[df['base_size'].str.contains('g')  & ~df['base_size'].str.contains('kg'), 'unit_value'] = df['size'] / 1000
    df.loc[df['base_size'].str.contains('ml',  case=False, na=False), 'unit_value'] = df['size'] / 1000
    df.loc[df['base_size'].str.contains('l')  & ~df['base_size'].str.contains('ml'), 'unit_value'] = df['size']

    # ── Create 'price_per_unit' column ────────────────────────────────────────
    df['price_per_unit'] = df['current_price'] / df['unit_value']

    # ── Create 'date' column ──────────────────────────────────────────────────
    df['date'] = datetime.now().strftime("%Y-%m-%d")

    # ── Create 'sku' column ───────────────────────────────────────────────────
    df['sku'] = df['category'] + df['brand'] + df['base_size']

    # ── Re-arrange columns ────────────────────────────────────────────────────
    df = df[[
        'name', 'category', 'brand', 'sku', 'store',
        'base_size', 'size', 'unit_type', 'unit_value',
        'old_price', 'current_price', 'discount',
        'price_per_unit', 'date', 'link'
    ]]

    # ── Save latest snapshot ──────────────────────────────────────────────────
    latest_path = PROCESSED_DIR / 'latest_items.csv'
    df.to_csv(latest_path, index=False)
    print(f"  💾 Latest snapshot saved → {latest_path}")

    # ── Append to price history ───────────────────────────────────────────────
    history_path = HISTORY_DIR / 'price_history.csv'
    try:
        old = pd.read_csv(history_path)
        df = pd.concat([old, df], ignore_index=True)
    except FileNotFoundError:
        pass

    df.drop_duplicates(subset=['sku', 'store', 'date'], inplace=True)
    df.to_csv(history_path, index=False)
    print(f"  💾 Price history saved  → {history_path}")
    print('✅ Cleaning completed successfully.')


if __name__ == "__main__":
    run_cleaning()