from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.db.connection import connect
from backend.surveys.questions import validate_and_normalize_answers
from backend.surveys.snapshot import build_survey_snapshot_record, replace_snapshot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild the single app survey Bronze snapshot from PostgreSQL.")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = read_completed_survey_rows()
    records = []
    skipped = []
    for row in rows:
        answers = json_object(row["answers"])
        try:
            _cleaned, normalized = validate_and_normalize_answers(survey_type=row["survey_type"], answers=answers)
        except ValueError:
            skipped.append(row["user_id"])
            continue
        records.append(
            build_survey_snapshot_record(
                survey_response_id=row["survey_response_id"],
                user_id=row["user_id"],
                survey_type=row["survey_type"],
                age=row["age"],
                gender=row["gender"],
                learner_type=row["learner_type"],
                submitted_at=row["created_at"],
                normalized_answers=normalized,
            )
        )

    summary = {"rows": len(rows), "records": len(records), "skipped": len(skipped), "dry_run": args.dry_run}
    print(json.dumps(summary, sort_keys=True))
    if args.dry_run or not records:
        return
    uri = replace_snapshot(records)
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE survey_responses
                SET exported_at = NOW(),
                    export_batch_id = %s,
                    export_object_uri = %s,
                    updated_at = NOW()
                WHERE user_id = ANY(%s::uuid[])
                """,
                ("survey_snapshot_rebuild_v1", uri, [record["user_id"] for record in records]),
            )
        connection.commit()
    print(json.dumps({"status": "success", "snapshot_uri": uri}, sort_keys=True))


def read_completed_survey_rows() -> list[dict]:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    sr.id::text AS survey_response_id,
                    sr.user_id::text AS user_id,
                    sr.survey_type,
                    sr.answers,
                    sr.created_at,
                    sp.age,
                    sp.gender,
                    sp.learner_type
                FROM survey_responses sr
                LEFT JOIN student_profiles sp ON sp.user_id = sr.user_id
                ORDER BY sr.created_at
                """
            )
            return [dict(row) for row in cursor.fetchall()]


def json_object(value: object) -> dict:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


if __name__ == "__main__":
    main()
