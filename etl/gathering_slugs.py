#!/usr/bin/env python
# coding: utf-8

import requests
import time
import random
import json

def get_all_data_slugs_final_v3():
    url = "https://example_website_xyz/offers"
    all_slugs = set()
    
    levels = ['junior', 'mid', 'senior', 'c_level']
    batch_size = 100
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://example_website_xyz/data',
    })

    print(" Starting")

    for level in levels:
        print(f"\n🔎 Downloading level: {level.upper()}")
        offset = 0
        
        while True:
            params = {
                'from': offset,
                'itemsCount': batch_size,
                'categories': 'data',
                'experienceLevels': level,  
                'sortBy': 'publishedAt',      
                'orderBy': 'descending',      
                'cityRadius': '30',           
                'currency': 'pln',            
                'keywordType': 'any',         
                'isPromoted': 'true'          
            }

            try:
                response = session.get(url, params=params, timeout=15)
                
                if response.status_code != 200:
                    print(f"Error {response.status_code}: {response.text[:100]}")
                    break
                
                data = response.json().get('data', [])

                if not data:
                    break

                new_batch = [o['slug'] for o in data if 'slug' in o]
                all_slugs.update(new_batch)
                
                print(f" Offset {offset}: +{len(new_batch)} (Unique: {len(all_slugs)})")
                
                offset += len(data)
                
                time.sleep(random.uniform(3, 5))

                if len(data) < batch_size:
                    break

            except Exception as e:
                print(f"Error: {e}")
                break
        
        time.sleep(random.uniform(5, 8))

    return list(all_slugs)



final_list = get_all_data_slugs_final_v3()

if final_list:
    with open('slugs_queue.json', 'w', encoding='utf-8') as f:
        json.dump(final_list, f, ensure_ascii=False, indent=4)
    print(f"\n SUCCESS! Downloaded {len(final_list)} unique slugs.")



import duckdb

con = duckdb.connect('data/data_job_market.db')

con.execute("CREATE SEQUENCE IF NOT EXISTS seq_slug_id START 1")

con.execute("""
    CREATE TABLE IF NOT EXISTS SLUGS (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_slug_id'),
        slug VARCHAR UNIQUE,
        processed INTEGER DEFAULT 0, -- 0: Niepobrane, 1: Pobrane
        created_at TIMESTAMP DEFAULT current_timestamp,
        updated_at TIMESTAMP DEFAULT current_timestamp
    )
""")

try:
    con.execute("""
        INSERT INTO SLUGS (slug)
        SELECT json FROM read_json_auto('slugs_queue.json')
        ON CONFLICT (slug) DO NOTHING
    """)
    print("Success! Data imported.")
except Exception as e:
    print(f"Error: {e}")

res = con.execute("SELECT * FROM SLUGS ORDER BY ID LIMIT 5").df()
display(res) 

con.close()

