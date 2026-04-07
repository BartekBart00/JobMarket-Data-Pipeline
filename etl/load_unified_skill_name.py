import os
import json
import argparse
import requests
import duckdb
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_PATH = 'data/data_job_market.db'
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_URL = "https://api.openai.com/v1/chat/completions"

PROMPT_VERSION = "v1"
MODEL_NAME = "gpt-4.1-nano"

if not OPENAI_API_KEY:
    raise ValueError("Brak OPENAI_API_KEY w pliku .env!")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Unify job skills using LLM.")
    parser.add_argument('--limit', type=int, default=None, 
                        help="Limit the number of raw skills to process (for testing).")
    parser.add_argument('--batch-size', type=int, default=100, 
                        help="Number of skills to send in one API request.")
    return parser.parse_args()

def get_clean_skills_from_ai(raw_skills_batch):
    """Sends a batch of skills to OpenAI using requests."""
    
    prompt = f"""
    You are an HR analyst. Below is a list of raw skill names from job postings.
    Your task is to extract clean, individual technologies from them (e.g., 'Python/R' -> ['Python', 'R']).
    Ignore difficulty levels and descriptions (e.g., 'Pyhon (mid)' -> ['Python']). Correct typos.
    
    Return the result as JSON in the format:
    {{
        "mappings": [
            {{"raw": "original_text", "clean": ["CleanTech1", "CleanTech2"]}}
        ]
    }}
    
    List to process:
    {json.dumps(raw_skills_batch)}
    """

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }

    payload = {
        "model": MODEL_NAME,
        "response_format": { "type": "json_object" },
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1
    }

    response = requests.post(OPENAI_URL, headers=headers, json=payload)
    response.raise_for_status()
    
    response_json = response.json()
    content = response_json['choices'][0]['message']['content']
    
    return json.loads(content), json.dumps(response_json)

def main():
    args = parse_arguments()
    con = duckdb.connect(DB_PATH)
    
    limit_clause = f"LIMIT {args.limit}" if args.limit else ""
    
    query = f"""
        SELECT DISTINCT skill_name AS raw_skill
        FROM silver.stg_offer_required_skills
        WHERE skill_name IS NOT NULL
          AND skill_name NOT IN (
              SELECT raw_skill 
              FROM silver.ai_skill_mapping 
              WHERE status = 'success'
          )
        {limit_clause}
    """
    
    try:
        df_pending = con.execute(query).df()
        unique_skills = df_pending['raw_skill'].tolist()
    except duckdb.CatalogException as e:
        print(f"Error: {e}. Make sure tables exist.")
        return

    if not unique_skills:
        print("No new records to map.")
        return

    print(f"Starting processing {len(unique_skills)} records...")

    for i in range(0, len(unique_skills), args.batch_size):
        batch = unique_skills[i:i + args.batch_size]
        print(f"Processing batch {i + 1} - {i + len(batch)} of {len(unique_skills)}...")
        
        records_to_insert = []
        now = datetime.now()

        try:
            ai_data, raw_json_payload = get_clean_skills_from_ai(batch)
            
            mappings_dict = {item.get('raw'): item.get('clean', []) for item in ai_data.get('mappings', [])}
            
            for raw_skill in batch:
                clean_list = mappings_dict.get(raw_skill)
                
                if clean_list is not None and len(clean_list) > 0:
                    for clean_skill in clean_list:
                        records_to_insert.append({
                            'raw_skill': raw_skill,
                            'clean_skill': clean_skill,
                            'llm_payload_json': raw_json_payload,
                            'prompt_version': PROMPT_VERSION,
                            'model_name': MODEL_NAME,
                            'status': 'success',
                            'error_message': None,
                            'created_at': now,
                            'processed_at': now
                        })
                else:
                    records_to_insert.append({
                        'raw_skill': raw_skill,
                        'clean_skill': None,
                        'llm_payload_json': raw_json_payload,
                        'prompt_version': PROMPT_VERSION,
                        'model_name': MODEL_NAME,
                        'status': 'error',
                        'error_message': 'LLM returned empty mapping or skipped this skill.',
                        'created_at': now,
                        'processed_at': now
                    })

        except Exception as e:
            print(f"Error for batch. Marking batch as error. Reason: {str(e)[:100]}...")
            error_payload = json.dumps({"batch": batch})
            
            for raw_skill in batch:
                records_to_insert.append({
                    'raw_skill': raw_skill,
                    'clean_skill': None,
                    'llm_payload_json': error_payload,
                    'prompt_version': PROMPT_VERSION,
                    'model_name': MODEL_NAME,
                    'status': 'error',
                    'error_message': str(e),
                    'created_at': now,
                    'processed_at': now
                })

        if records_to_insert:
            df_batch = pd.DataFrame(records_to_insert)
            
            placeholders = ', '.join(['?'] * len(batch))
            delete_query = f"DELETE FROM silver.ai_skill_mapping WHERE status = 'error' AND raw_skill IN ({placeholders})"
            con.execute(delete_query, batch)
            
            insert_query = """
                INSERT INTO silver.ai_skill_mapping (
                    raw_skill, clean_skill, llm_payload_json, prompt_version, 
                    model_name, status, error_message, created_at, processed_at
                )
                SELECT raw_skill, clean_skill, llm_payload_json, prompt_version, 
                       model_name, status, error_message, created_at, processed_at
                FROM df_batch
            """
            con.execute(insert_query)

    print("Finished mapping.")
    con.close()

if __name__ == "__main__":
    main()