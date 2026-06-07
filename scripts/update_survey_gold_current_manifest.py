from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime

from google.cloud import storage


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
MANIFEST_OBJECT = "gold/dashboard_tables/_manifests/survey_current.json"
TABLE_PREFIXES = {
    "survey_overview_summary": "gold/dashboard_tables/survey_overview_summary/",
    "survey_response_by_date": "gold/dashboard_tables/survey_response_by_date/",
    "survey_demographic_summary": "gold/dashboard_tables/survey_demographic_summary/",
    "survey_question_distribution": "gold/dashboard_tables/survey_question_distribution/",
    "survey_numeric_summary": "gold/dashboard_tables/survey_numeric_summary/",
    "survey_analytic_features": "gold/dashboard_tables/survey_analytic_features/",
}
REQUIRED_TABLES = {"survey_analytic_features"}


def run_id_from_name(name: str) -> str:
    for segment in name.split("/"):
        if segment.startswith("run_id="):
            return segment.removeprefix("run_id=")
    return ""


def main() -> int:
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    runs: dict[str, dict[str, list[dict[str, object]]]] = defaultdict(lambda: defaultdict(list))
    for table, prefix in TABLE_PREFIXES.items():
        for blob in client.list_blobs(bucket, prefix=prefix):
            if not blob.name.endswith(".parquet") or not blob.size:
                continue
            run_id = run_id_from_name(blob.name)
            if not run_id:
                continue
            runs[run_id][table].append(
                {
                    "name": blob.name,
                    "generation": int(blob.generation or 0),
                    "size": int(blob.size or 0),
                }
            )

    complete = [
        (
            max(item["generation"] for table in REQUIRED_TABLES for item in tables[table]),
            run_id,
            tables,
        )
        for run_id, tables in runs.items()
        if REQUIRED_TABLES.issubset(tables)
    ]
    if not complete:
        raise RuntimeError("No complete versioned Survey Gold run was found.")

    _generation, run_id, tables = max(complete, key=lambda item: item[0])
    payload = {
        "schema_version": "survey_gold_manifest_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "required_tables": sorted(REQUIRED_TABLES),
        "tables": {table: entries for table, entries in sorted(tables.items())},
    }
    bucket.blob(MANIFEST_OBJECT).upload_from_string(
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        content_type="application/json",
    )
    print(json.dumps({"status": "success", "manifest": f"gs://{BUCKET_NAME}/{MANIFEST_OBJECT}", **payload}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
