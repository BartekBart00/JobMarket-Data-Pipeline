import argparse
import json
import os
from pathlib import Path
from typing import Any

import duckdb
import requests
from etl.create_raw_llm_enrichment_table import ensure_raw_llm_enrichment_table


DB_PATH = Path("data/data_job_market.db")
DEFAULT_MODEL = "gpt-4.1-nano"
DEFAULT_PROMPT_VERSION = "v1"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"
ENV_PATH = Path(".env")


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def get_unprocessed_offers(
    connection: duckdb.DuckDBPyConnection,
    prompt_version: str,
    model_name: str,
    limit: int,
    slugs: list[str] | None = None,
) -> list[tuple[Any, str, str | None]]:
    if slugs:
        placeholders = ", ".join(["?"] * len(slugs))
        query = f"""
        SELECT ro.id, ro.slug, ro.body
        FROM bronze.raw_offers AS ro
        LEFT JOIN bronze.raw_llm_enrichment AS re
          ON ro.slug = re.slug
         AND re.prompt_version = ?
         AND re.model_name = ?
         AND re.status = 'success'
        WHERE re.slug IS NULL
          AND ro.body IS NOT NULL
          AND length(trim(ro.body)) > 0
          AND ro.slug IN ({placeholders})
        ORDER BY ro.slug
        LIMIT ?
        """
        params: list[Any] = [prompt_version, model_name, *slugs, limit]
        return connection.execute(query, params).fetchall()

    query = """
    SELECT ro.id, ro.slug, ro.body
    FROM bronze.raw_offers AS ro
    LEFT JOIN bronze.raw_llm_enrichment AS re
      ON ro.slug = re.slug
     AND re.prompt_version = ?
     AND re.model_name = ?
     AND re.status = 'success'
    WHERE re.slug IS NULL
      AND ro.body IS NOT NULL
      AND length(trim(ro.body)) > 0
    ORDER BY ro.slug
    LIMIT ?
    """
    return connection.execute(query, [prompt_version, model_name, limit]).fetchall()


def build_prompt(offer_body: str) -> str:
    return (
        "Extract up to 6 IT technical skills from this job offer body. "
        "Return only valid JSON with exact shape: "
        '{"skills":[{"name":"string","category":"string"}]}. '
        "Use only skill names explicitly present in the offer text (1:1 wording from offer), "
        "no inference, no expansion, no guessed related tools. "
        "Deduplicate variants (for example Databricks and Databricks functions -> Databricks). "
        "Keep only technologies/tools/frameworks/platforms/databases/cloud/devops terms related to IT. "
        "Ignore languages (like English/Polish), soft skills, teamwork, communication, leadership, "
        "management, business/domain knowledge, and generic non-technical requirements. "
        "If the offer contains fewer than 6 valid skills, return only those present. "
        "Do not include markdown or extra keys.\n\n"
        f"OFFER_BODY:\n{offer_body}"
    )


def call_llm(offer_body: str, model_name: str, api_key: str, timeout_seconds: int = 60) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model_name,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": "You are an information extraction engine.",
            },
            {
                "role": "user",
                "content": build_prompt(offer_body),
            },
        ],
    }
    response = requests.post(OPENAI_URL, headers=headers, json=payload, timeout=timeout_seconds)
    response.raise_for_status()
    raw = response.json()
    content = raw["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    return parsed


def sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    skills_raw = payload.get("skills", [])
    if not isinstance(skills_raw, list):
        raise ValueError("Invalid payload shape: 'skills' must be a list.")

    cleaned_skills: list[dict[str, str]] = []
    seen_names: set[str] = set()

    for skill in skills_raw:
        if not isinstance(skill, dict):
            continue
        name = skill.get("name")
        category = skill.get("category")

        if not isinstance(name, str):
            continue
        name_clean = name.strip()
        if not name_clean:
            continue

        name_key = name_clean.lower()
        if name_key in seen_names:
            continue

        category_clean = category.strip() if isinstance(category, str) else "other"
        if not category_clean:
            category_clean = "other"

        cleaned_skills.append({"name": name_clean, "category": category_clean})
        seen_names.add(name_key)

        if len(cleaned_skills) >= 6:
            break

    return {"skills": cleaned_skills}


def is_valid_payload(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    skills = payload.get("skills")
    if not isinstance(skills, list):
        return False
    if len(skills) > 6:
        return False
    for skill in skills:
        if not isinstance(skill, dict):
            return False
        name = skill.get("name")
        if not isinstance(name, str) or not name.strip():
            return False
        category = skill.get("category")
        if not isinstance(category, str) or not category.strip():
            return False
        if "confidence" in skill:
            return False
    return True


def insert_success(
    connection: duckdb.DuckDBPyConnection,
    offer_id: Any,
    slug: str,
    payload: dict[str, Any],
    prompt_version: str,
    model_name: str,
) -> None:
    connection.execute(
        """
        INSERT INTO bronze.raw_llm_enrichment
        (offer_id, slug, llm_payload_json, prompt_version, model_name, status, error_message)
        VALUES (?, ?, ?, ?, ?, 'success', NULL)
        """,
        [offer_id, slug, json.dumps(payload), prompt_version, model_name],
    )


def insert_error(
    connection: duckdb.DuckDBPyConnection,
    offer_id: Any,
    slug: str,
    prompt_version: str,
    model_name: str,
    error_message: str,
) -> None:
    connection.execute(
        """
        INSERT INTO bronze.raw_llm_enrichment
        (offer_id, slug, llm_payload_json, prompt_version, model_name, status, error_message)
        VALUES (?, ?, ?, ?, ?, 'error', ?)
        """,
        [offer_id, slug, json.dumps({"skills": []}), prompt_version, model_name, error_message[:2000]],
    )


def run(limit: int, prompt_version: str, model_name: str, slugs: list[str] | None = None) -> None:
    load_env_file(ENV_PATH)
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError("Missing OPENAI_API_KEY environment variable.")

    with duckdb.connect(str(DB_PATH)) as connection:
        ensure_raw_llm_enrichment_table(connection)
        rows = get_unprocessed_offers(connection, prompt_version, model_name, limit, slugs=slugs)
        if not rows:
            print("No new offers to process.")
            return

        print(f"Processing {len(rows)} offers...")
        success_count = 0
        error_count = 0

        for offer_id, slug, body in rows:
            try:
                payload = call_llm(body or "", model_name=model_name, api_key=api_key)
                payload = sanitize_payload(payload)
                if not is_valid_payload(payload):
                    raise ValueError("Invalid payload shape: expected {'skills': [{'name','category'}]} with max 6.")
                insert_success(connection, offer_id, slug, payload, prompt_version, model_name)
                connection.commit()
                success_count += 1
                print(f"[success] {slug}")
            except Exception as exc:
                insert_error(connection, offer_id, slug, prompt_version, model_name, str(exc))
                connection.commit()
                error_count += 1
                print(f"[error] {slug} -> {exc}")

        print(f"Done. success={success_count}, error={error_count}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate bronze.raw_llm_enrichment incrementally.")
    parser.add_argument("--limit", type=int, default=25, help="Max number of offers per run.")
    parser.add_argument("--prompt-version", type=str, default=DEFAULT_PROMPT_VERSION)
    parser.add_argument("--model-name", type=str, default=DEFAULT_MODEL)
    parser.add_argument(
        "--slugs",
        type=str,
        default="",
        help="Comma-separated slug list for targeted tests, e.g. slug-1,slug-2",
    )
    args = parser.parse_args()
    slugs = [slug.strip() for slug in args.slugs.split(",") if slug.strip()] or None
    run(limit=args.limit, prompt_version=args.prompt_version, model_name=args.model_name, slugs=slugs)


if __name__ == "__main__":
    main()

