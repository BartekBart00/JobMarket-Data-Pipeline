#!/usr/bin/env python
# coding: utf-8


import duckdb
import os
import shutil
from pathlib import Path

# Paths configuration
BASE_DIR = Path("offers")
UNPROCESSED_DIR = BASE_DIR / "unprocessed"
PROCESSED_DIR = BASE_DIR / "processed"
DB_PATH = "data/data_job_market.db"

def ingest_bronze():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    
    files = list(UNPROCESSED_DIR.glob("*.json"))
    
    if not files:
        print("No new files.")
        return

    print(f"Found {len(files)} files. Loading...")

    try:
        first_file = str(files[0])
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS bronze.raw_offers AS 
            SELECT * FROM read_json_auto('{first_file}') WHERE 1=0
        """)

        unprocessed_pattern = str(UNPROCESSED_DIR / "*.json")
        con.execute(f"""
            INSERT INTO bronze.raw_offers 
            SELECT * FROM read_json_auto('{unprocessed_pattern}')
        """)
        
        print("Data loaded successfully to bronze.raw_offers.")

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        for file_path in files:
            shutil.move(str(file_path), str(PROCESSED_DIR / file_path.name))
        
        print(f"Moved {len(files)} files to processed folder.")

    except Exception as e:
        print(f"Error during processing: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    ingest_bronze()

