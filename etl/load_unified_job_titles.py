import argparse
import json
import os
from pathlib import Path
from typing import Any

import duckdb
import requests
from etl.create_raw_llm_enrichment_table import ensure_raw_offer_unified_title_table

DB_PATH = Path("data/data_job_market.db")
DEFAULT_MODEL = "gpt-4.1-nano"
DEFAULT_PROMPT_VERSION = "v1-unified-title"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"
ENV_PATH = Path(".env")

ALLOWED_UNIFIED_TITLES: tuple[str, ...] = (
    "Data Engineer",
    "Database Developer",
    "Data Analyst",
    "BI Analyst",
    "Software Engineer (Data)",
    "Data Specialist",
    "Data Platform Engineer",
    "Machine Learning Engineer",
    "Data Manager",
    "Data Architect",
    "Data Scientist",
    "Data Consultant",
    "Database Administrator",
    "Data Governance Specialist",
    "MLOps Engineer",
    "BI Developer",
    "Data Modeler",
    "Data Product Manager",
    "Digital/Web Analyst",
)

_ALLOWED_BY_LOWER: dict[str, str] = {t.lower(): t for t in ALLOWED_UNIFIED_TITLES}


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


def normalize_unified_title(raw: str | None) -> str | None:
    if raw is None or not isinstance(raw, str):
        return None
    s = raw.strip()
    if s in ALLOWED_UNIFIED_TITLES:
        return s
    return _ALLOWED_BY_LOWER.get(s.lower())


def get_unprocessed_offers(
    connection: duckdb.DuckDBPyConnection,
    prompt_version: str,
    model_name: str,
    limit: int,
    slugs: list[str] | None = None,
) -> list[tuple[Any, str, str | None, str | None, str | None]]:
    if slugs:
        placeholders = ", ".join(["?"] * len(slugs))
        query = f"""
        SELECT
            ro.id,
            ro.slug,
            ro.title,
            ro.experienceLevel,
            to_json(ro.requiredSkills) AS required_skills_json
        FROM bronze.raw_offers AS ro
        LEFT JOIN bronze.raw_offer_unified_title AS ut
          ON ro.id = ut.offer_id
         AND ut.prompt_version = ?
         AND ut.model_name = ?
         AND ut.status = 'success'
        WHERE ut.offer_id IS NULL
          AND ro.slug IN ({placeholders})
        ORDER BY ro.slug
        LIMIT ?
        """
        params: list[Any] = [prompt_version, model_name, *slugs, limit]
        return connection.execute(query, params).fetchall()

    query = """
    SELECT
        ro.id,
        ro.slug,
        ro.title,
        ro.experienceLevel,
        to_json(ro.requiredSkills) AS required_skills_json
    FROM bronze.raw_offers AS ro
    LEFT JOIN bronze.raw_offer_unified_title AS ut
      ON ro.id = ut.offer_id
     AND ut.prompt_version = ?
     AND ut.model_name = ?
     AND ut.status = 'success'
    WHERE ut.offer_id IS NULL
    ORDER BY ro.slug
    LIMIT ?
    """
    return connection.execute(query, [prompt_version, model_name, limit]).fetchall()


def build_prompt(title: str, experience_level: str | None, required_skills_json: str | None) -> str:
    allowed_block = "\n".join(f"- {t}" for t in ALLOWED_UNIFIED_TITLES)
    return (
        "You classify a job offer into exactly ONE unified job title from the list below.\n"
        "Use the fields: job title, experience level, and required skills (JSON array) only.\n"
        "Pick the single best-matching label. If several fit, choose the most specific one for data/IT work.\n"
        "The unified_title value MUST be exactly one of the allowed strings (same spelling and punctuation).\n\n"
        "Return only valid JSON: {\"unified_title\":\"...\"}\n"
        "No markdown, no extra keys.\n\n"
        "ALLOWED UNIFIED TITLES:\n"
        f"{allowed_block}\n\n"
        f"job_title: {title or ''}\n"
        f"experience_level: {experience_level or ''}\n"
        f"required_skills_json: {required_skills_json or '[]'}\n"
    )


def call_llm(
    title: str,
    experience_level: str | None,
    required_skills_json: str | None,
    model_name: str,
    api_key: str,
    timeout_seconds: int = 60,
) -> dict[str, Any]:
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
                "content": "You map job postings to a fixed taxonomy of unified job titles.",
            },
            {
                "role": "user",
                "content": build_prompt(title, experience_level, required_skills_json),
            },
        ],
    }
    response = requests.post(OPENAI_URL, headers=headers, json=payload, timeout=timeout_seconds)
    response.raise_for_status()
    raw = response.json()
    content = raw["choices"][0]["message"]["content"]
    return json.loads(content)


def insert_success(
    connection: duckdb.DuckDBPyConnection,
    offer_id: Any,
    slug: str,
    unified_title: str,
    payload: dict[str, Any],
    prompt_version: str,
    model_name: str,
) -> None:
    connection.execute(
        """
        INSERT INTO bronze.raw_offer_unified_title
        (offer_id, slug, unified_title, llm_payload_json, prompt_version, model_name, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, 'success', NULL)
        """,
        [offer_id, slug, unified_title, json.dumps(payload), prompt_version, model_name],
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
        INSERT INTO bronze.raw_offer_unified_title
        (offer_id, slug, unified_title, llm_payload_json, prompt_version, model_name, status, error_message)
        VALUES (?, ?, NULL, ?, ?, ?, 'error', ?)
        """,
        [
            offer_id,
            slug,
            json.dumps({"unified_title": None}),
            prompt_version,
            model_name,
            error_message[:2000],
        ],
    )


def run(limit: int, prompt_version: str, model_name: str, slugs: list[str] | None = None) -> None:
    load_env_file(ENV_PATH)
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError("Missing OPENAI_API_KEY environment variable.")

    with duckdb.connect(str(DB_PATH)) as connection:
        ensure_raw_offer_unified_title_table(connection)
        rows = get_unprocessed_offers(connection, prompt_version, model_name, limit, slugs=slugs)
        if not rows:
            print("No new offers to process.")
            return

        print(f"Processing {len(rows)} offers...")
        success_count = 0
        error_count = 0

        for offer_id, slug, title, experience_level, required_skills_json in rows:
            try:
                payload = call_llm(
                    title or "",
                    experience_level,
                    required_skills_json,
                    model_name=model_name,
                    api_key=api_key,
                )
                raw_title = payload.get("unified_title") if isinstance(payload, dict) else None
                canonical = normalize_unified_title(raw_title if isinstance(raw_title, str) else None)
                if not canonical:
                    raise ValueError(
                        f"unified_title must be one of the allowed labels; got: {raw_title!r}"
                    )
                out_payload = {"unified_title": canonical}
                insert_success(
                    connection,
                    offer_id,
                    slug,
                    canonical,
                    out_payload,
                    prompt_version,
                    model_name,
                )
                connection.commit()
                success_count += 1
                print(f"[success] {slug} -> {canonical}")
            except Exception as exc:
                insert_error(connection, offer_id, slug, prompt_version, model_name, str(exc))
                connection.commit()
                error_count += 1
                print(f"[error] {slug} -> {exc}")

        print(f"Done. success={success_count}, error={error_count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Populate bronze.raw_offer_unified_title from title, experienceLevel, requiredSkills."
    )
    parser.add_argument("--limit", type=int, default=1000, help="Max number of offers per run.")
    parser.add_argument("--prompt-version", type=str, default=DEFAULT_PROMPT_VERSION)
    parser.add_argument("--model-name", type=str, default=DEFAULT_MODEL)
    parser.add_argument(
        "--slugs",
        type=str,
        default="",
        help="Comma-separated slug list for tests.",
    )
    args = parser.parse_args()
    slugs = [s.strip() for s in args.slugs.split(",") if s.strip()] or None
    run(limit=args.limit, prompt_version=args.prompt_version, model_name=args.model_name, slugs=slugs)


if __name__ == "__main__":
    main()
