from pathlib import Path

import duckdb

DB_PATH = Path("data/data_job_market.db")
RAW_LLM_ENRICHMENT_DDL = """
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_llm_enrichment (
    id UUID DEFAULT uuid(),
    slug VARCHAR NOT NULL,
    offer_id UUID,
    llm_payload_json JSON NOT NULL,
    prompt_version VARCHAR NOT NULL,
    model_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'success',
    error_message VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT raw_llm_enrichment_status_check CHECK (status IN ('success', 'error'))
);
"""

RAW_OFFER_UNIFIED_TITLE_DDL = """

CREATE TABLE IF NOT EXISTS bronze.raw_offer_unified_title (
    id UUID DEFAULT uuid(),
    offer_id UUID NOT NULL,
    slug VARCHAR NOT NULL,
    unified_title VARCHAR,
    llm_payload_json JSON NOT NULL,
    prompt_version VARCHAR NOT NULL,
    model_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'success',
    error_message VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT raw_offer_unified_title_status_check CHECK (status IN ('success', 'error'))
);

CREATE INDEX IF NOT EXISTS idx_raw_offer_unified_title_offer_id
    ON bronze.raw_offer_unified_title (offer_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_offer_unified_title_run
    ON bronze.raw_offer_unified_title (offer_id, prompt_version, model_name, status);
"""

RAW_AI_SKILL_MAPPING_DDL = """

CREATE TABLE IF NOT EXISTS silver.ai_skill_mapping (
    id UUID DEFAULT uuid(),
    raw_skill VARCHAR NOT NULL,          -- oryginalna nazwa z ogłoszenia
    clean_skill VARCHAR,                 -- zunifikowana nazwa przez LLM
    llm_payload_json JSON NOT NULL,      -- zrzut odpowiedzi/zapytania dla debuggowania
    prompt_version VARCHAR NOT NULL,     -- wersja promptu
    model_name VARCHAR NOT NULL,         -- nazwa użytego modelu
    status VARCHAR NOT NULL DEFAULT 'success', 
    error_message VARCHAR,               -- treść błędu (jeśli wystąpił)
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT ai_skill_mapping_status_check CHECK (status IN ('success', 'error'))
);
"""

def ensure_raw_llm_enrichment_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(RAW_LLM_ENRICHMENT_DDL)

def ensure_raw_offer_unified_title_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(RAW_OFFER_UNIFIED_TITLE_DDL)

def ensure_raw_ai_skill_mapping_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(RAW_AI_SKILL_MAPPING_DDL)

def create_raw_llm_enrichment_table() -> None:
    with duckdb.connect(str(DB_PATH)) as connection:
        ensure_raw_llm_enrichment_table(connection)
        ensure_raw_offer_unified_title_table(connection)
        ensure_raw_ai_skill_mapping_table(connection)

def main() -> None:
    create_raw_llm_enrichment_table()
    print("Tables bronze.raw_llm_enrichment and bronze.raw_offer_unified_title are ready.")

if __name__ == "__main__":
    main()
