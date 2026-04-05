# Import required library
import pandas as pd
from pathlib import Path

# Create a function to run analysis
def run_analysis():
    BASE_DIR = Path(__file__).resolve().parents[2]

    file_path = BASE_DIR / 'data' / 'processed' / 'latest_items.csv'
    print("Reading from:", file_path)
    df = pd.read_csv(file_path)

    # Pivots
    pivot = df.pivot_table(
        index = 'sku',
        columns = 'store',
        values = 'price_per_unit',
        aggfunc = 'median'
    ).dropna()
 
    # Calculate the price index
    median = pivot.median(axis=1) # median across stores
    price_index = pivot.div(median, axis=0)
    store_index = price_index.mean()

    print(pivot.shape)
    print('\nStore index\n\n', store_index)

    # Save outputs
    analytics_path = BASE_DIR / 'data' / 'analytics'
    analytics_path.mkdir(parents=True, exist_ok=True)

    pivot.to_csv(analytics_path / 'price_per_unit_pivot.csv')
    store_index.to_csv(analytics_path / 'store_index.csv')

if __name__ == '__main__':
    run_analysis()