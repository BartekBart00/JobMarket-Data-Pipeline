#!/usr/bin/env python
# coding: utf-8

import requests
import json
import time
import random
import duckdb
from pathlib import Path
from datetime import datetime

# Configuration paths
DB_PATH = 'data_job_market.db'
OFFERS_DIR = Path("offers/unprocessed")
OFFERS_DIR.mkdir(exist_ok=True) # If not exists

def fetch_offer(slug):
    api_url = f"https://example_website_xyz/{slug}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pl,en-US;q=0.7,en;q=0.3',
        'Referer': f'https://example_website_xyz/job-offer/{slug}',
        'Origin': 'https://example_website_xyz.com',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }

    try:
        response = requests.get(api_url, headers=headers, timeout=15)
        
        if response.status_code == 429:
            print("Too many requests. 2 minute break...")
            time.sleep(120)
            return None

        response.raise_for_status()
        return response.json()

    except Exception as e:
        print(f"Error while fetching {slug}: {e}")
        return None

def process_queue():
    con = duckdb.connect(DB_PATH)
    
    queue = con.execute("SELECT slug FROM SLUGS WHERE processed = 0").fetchall()
    
    if not queue:
        print("No offers to download")
        con.close()
        return

    print(f"{len(queue)} offers to download.")

    for (slug,) in queue:
        data = fetch_offer(slug)
        
        if data:
            safe_filename = f"{slug.replace('/', '_')}.json"
            file_path = OFFERS_DIR / safe_filename
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            con.execute("""
                UPDATE SLUGS 
                SET processed = 1, 
                    updated_at = CURRENT_TIMESTAMP 
                WHERE slug = ?
            """, [slug])
            
            print(f"Saved: {slug}")
        
        else:
            print(f"Skipping {slug} due to error.")

        wait_time = random.uniform(3, 8)
        print(f"Waiting {wait_time:.2f}s...")
        time.sleep(wait_time)

    con.close()
    print("Finished scraping session.")

if __name__ == "__main__":
    process_queue()

