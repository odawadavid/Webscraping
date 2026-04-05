# Import required libraries
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path

def run_emart():

    BASE_DIR = Path(__file__).resolve().parents[2]
    file_path = BASE_DIR / 'data' / 'raw' / 'emart.csv'
    
    # Create a container for all products
    all_products = []
    
    # Selected items (staple basket)
    categories = {
        'Sugar': "https://e-mart.co.ke/index.php?category_id=0&search=white+sugar&submit_search=&route=product%2Fsearch",
        "Milk": "https://e-mart.co.ke/index.php?category_id=0&search=whole+milk&submit_search=&route=product%2Fsearch",
        "Wheat Flour": "https://e-mart.co.ke/index.php?category_id=0&search=wheat+flour&submit_search=&route=product%2Fsearch",
        "Bread": "https://e-mart.co.ke/index.php?route=product/search&search=loaf&category_id=0",
        "Cooking Oil": "https://e-mart.co.ke/index.php?category_id=0&search=cooking+oil&submit_search=&route=product%2Fsearch",
        "Rice": "https://e-mart.co.ke/index.php?category_id=0&search=rice&submit_search=&route=product%2Fsearch",
        "Ugali": "https://e-mart.co.ke/index.php?category_id=0&search=maize+meal&submit_search=&route=product%2Fsearch"
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }
    
    # Create a function to extract a product
    def extract_emart(product, base_url):
        """
        Scrape a single product
        """
        link_tag = product.select_one('h4 a')
        link = urljoin(base_url, link_tag.get('href'))
    
        return {
            'name': product.select_one('h4 a').get_text(strip=True),
            'current_price': product.select_one('span.price-new').get_text(strip=True),
            'link': link
        }
    
    for category, url in categories.items():
        print(f'\nScraping {category}...')
        print(f'Searching {url}')
    
        for page in range(1,11):
            page_url = f'{url}&page={page}'
            response = requests.get(page_url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
    
            products = soup.select('.product-layout')
            
            if not products:
                break
                
            print(f'Found {len(products)} items on page {page}.')
    
            for category_url in categories.values():
                for product in products:
                    items = extract_emart(product, category_url)
                    if items:
                        all_products.append(items)
    
    df = pd.DataFrame(all_products)
    df.to_csv(file_path, index=False)
    print('\n\nFile successfully saved at:\n', file_path)

if __name__ == '__main__':
    run_emart()
