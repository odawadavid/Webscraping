# Import the required modules
import pandas as pd, re
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path
pd.set_option('display.max_colwidth', None)

def run_cleaning():
    BASE_DIR = Path(__file__).resolve().parents[2]

    # Load data
    naivas_df = pd.read_csv(BASE_DIR / 'data' / 'raw' / 'naivas.csv')
    quickmart_df = pd.read_csv(BASE_DIR / 'data' / 'raw' / 'quickmart.csv')
    emart_df = pd.read_csv(BASE_DIR / 'data' / 'raw' / 'emart.csv')
    
    df = pd.concat([naivas_df, quickmart_df, emart_df])
    
    # Clean 'name' column
    df['name'] = df['name'].str.lower().str.strip()
    
    # Create a 'brand' column
    df['brand'] = df['name'].str.capitalize().str.split().str[0]
    
    # Create the 'store' column
    def extract_store(url):
        try:
            domain = urlparse(url).netloc
            if 'naivas' in domain:
                return 'Naivas'
            elif 'quickmart' in domain:
                return 'Quickmart'
            elif 'e-mart' in domain or 'co.ke' in domain:
                return 'E-Mart'
        except:
            return None
    df['store'] = df['link'].apply(extract_store)
    
    # Create a 'base_size' column |(Volume/Weight matching)
    df['base_size'] = df['name'].str.extract(
        r'(\d+\s?(?:ml|l|ltr|g|kg))',
        flags = re.IGNORECASE
    )
    
    df['base_size'] = df['base_size'].str.lower().str.replace(' ', '', regex=False)
    
    # Create a product 'category' column
    categories = {
        r'sugar': 'Sugar',
        r'bread': 'Bread',
        r'milk|uht': 'Milk',
        r'rice': 'Rice',
        r'cooking oil|vegetable oil|oil': 'Cooking Oil',
        r'wheat|flour|baking': 'Wheat Flour',
        r'maize meal|maize flour|ugali': 'Maize Meal'
    }
    
    df['category'] = 'Other'
    for pattern, value in categories.items():
        df.loc[df['name'].str.contains(pattern, regex=True, na=False), 'category'] = value
    
    # Filter out products not categorized and drop the 'search' column
    df = df[df['category'] != 'Other']
    if 'search' in df.columns:
        df = df.drop(columns=['search'])
        
    # Clean the prices
    def clean_prices(col):
        return (
            col.str
            .replace('KES ', '', regex=False).str
            .replace('\xa0','', regex=False).str
            .replace('KES', '', regex=False).str
            .replace(',', '', regex=False).str
            .replace('\n', '', regex=False).str
            .replace('No discount', '0').str   
            .strip()
        )
    
    df[['current_price', 'old_price']] = df[['current_price', 'old_price']].apply(clean_prices)
    
    vals_curr = df['current_price'].str.findall(r'\d+\.?\d*')
    vals_old = df['old_price'].str.findall(r'\d+\.?\d*')
    
    df['current_price'] = vals_curr.str[0].astype(float)
    df['old_price'] = vals_old.str[0].astype(float)
    
    # Fill the missing values in 'discount' column
    df['old_price'] = df['old_price'].fillna(0)
    
    # Clean the 'base_size' column
    df = df[(df['base_size'] != '') & (df['base_size'] != '06800') & df['base_size'].notna()]

    # Create the 'size' column
    df['size'] = df['base_size'].str.replace(r'(?:ml|l|ltr|g|kg)', '', regex=True).astype('float64')
    
    # Create a unit type column
    df['unit_type'] = None
    
    df.loc[df['base_size'].str.contains('kg', case=False, na=False), 'unit_type'] = 'kg'
    df.loc[df['base_size'].str.contains('g') & ~df['base_size'].str.contains('kg'), 'unit_type'] = 'kg'
    
    df.loc[df['base_size'].str.contains('ml', case=False, na=False), 'unit_type'] = 'litres'
    df.loc[df['base_size'].str.contains('l') & ~df['base_size'].str.contains('ml'), 'unit_type'] = 'litres'
    
    # Create a 'discount' column
    df['discount'] = (df['old_price'] - df['current_price']).where(df['old_price'] > 0, 0)
    
    
    # Create a 'unit_value' column
    df = df.reset_index(drop=True)
    
    df['unit_value'] = None
    
    df.loc[df['base_size'].str.contains('kg', case=False, na=False), 'unit_value'] = df['size']
    df.loc[df['base_size'].str.contains('g') & ~df['base_size'].str.contains('kg'), 'unit_value'] = df['size'] / 1000
    
    df.loc[df['base_size'].str.contains('ml', case=False, na=False), 'unit_value'] = df['size'] / 1000
    df.loc[df['base_size'].str.contains('l') & ~df['base_size'].str.contains('ml'), 'unit_value'] = df['size']
    
    # Create a 'price_per_unit'
    df['price_per_unit'] = df['current_price'] / df['unit_value']

    # Create a 'date' column
    df["date"] = datetime.now().strftime("%Y-%m-%d")

    # Create a product ID
    df['sku'] = (
        df['category'] + '' +
        df['brand'] + '' +
        df['base_size']
    )
    
    # Re-arrange the columns
    df = df[['name', 'category', 'brand', 'sku', 'store', 'base_size', 'size', 'unit_type', 'unit_value', 'old_price', 'current_price', 'discount', 'price_per_unit', 'date', 'link']]

 
    # Save latest snapshots
    df.to_csv('C:/Users/admin/OneDrive/Desktop/Webscraping/data/processed/latest_items.csv', index=False)
    
    # append to history
    try:
        old = pd.read_csv("C:/Users/admin/OneDrive/Desktop/Webscraping/data/history/price_history.csv")
        df = pd.concat([old, df], ignore_index=True)
    except FileNotFoundError:
        pass

    # Fix the de-duplication isue
    df.drop_duplicates(subset=["sku", "store", "date"], inplace=True)
    
    df.to_csv("C:/Users/admin/OneDrive/Desktop/Webscraping/data/history/price_history.csv", index=False)
    print('Successfully ran the cleaning...')


if __name__ == "__main__":
    run_cleaning()


