from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import pandas as pd
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.surveys.definitions import load_survey_questions
from backend.surveys.mapping import dashboard_columns_for_answer
from backend.surveys.snapshot import SCHEMA_VERSION, replace_snapshot

load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_BUCKET = os.getenv("GCS_BUCKET_NAME") or os.getenv("GCS_BUCKET") or "student-mental-health-lake-nhom1-2026"
SURVEY_BRONZE_PREFIX = "bronze/app_survey_snapshot"
USER_PROFILE_BRONZE_PREFIX = "bronze/app_user_profiles"
CHAT_SESSION_BRONZE_PREFIX = "bronze/app_chat_session_users"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export PostgreSQL operational snapshots to GCS Bronze Parquet.")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--export-date", default=date.today().isoformat())
    parser.add_argument("--full-survey", action="store_true", help="Export all survey responses instead of only unexported rows.")
    parser.add_argument("--dry-run", action="store_true", help="Read PostgreSQL and print counts without uploading or marking exported.")
    return parser.parse_args()


def connect():
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError("Missing psycopg. Install project requirements first.") from exc

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")
    return psycopg.connect(database_url, row_factory=dict_row)


def storage_bucket(bucket_name: str):
    from google.cloud import storage

    client = storage.Client()
    return client.bucket(bucket_name)


def hash_user_id(user_id: object) -> str:
    from backend.chat_logs.gcs_writer import hash_user_id as hash_value

    return hash_value(str(user_id))


def read_survey_responses(connection, full_survey: bool, export_date: str) -> pd.DataFrame:
    where_clause = "" if full_survey else "WHERE sr.exported_at IS NULL"
    query = f"""
        SELECT
            sr.id::text AS survey_response_id,
            sr.user_id::text AS user_id,
            sr.survey_type,
            sr.answers,
            sr.created_at,
            sr.updated_at,
            sp.survey_required,
            sp.survey_completed,
            sp.survey_type AS profile_survey_type,
            sp.survey_postponed,
            sp.survey_completed_at,
            sp.age,
            sp.gender,
            sp.learner_type
        FROM survey_responses sr
        JOIN app_users u ON u.id = sr.user_id
        LEFT JOIN student_profiles sp ON sp.user_id = u.id
        {where_clause}
        ORDER BY sr.created_at
    """
    frame = query_frame(connection, query)
    if frame.empty:
        return frame
    frame["user_id_hash"] = frame["user_id"].map(hash_user_id)
    frame["audience_group"] = frame["age"].map(lambda value: "school" if pd.notna(value) and int(value) <= 18 else "university" if pd.notna(value) else "unknown")
    frame["answers_json"] = frame["answers"].map(lambda value: json.dumps(_json_object(value), ensure_ascii=False, sort_keys=True))
    mapped_rows = frame.apply(lambda row: _mapped_survey_answers(row, export_date), axis=1)
    mapped_frame = pd.DataFrame(mapped_rows.tolist(), index=frame.index)
    if not mapped_frame.empty:
        frame = pd.concat([frame, mapped_frame], axis=1)
    return frame.drop(columns=["answers"])


def read_user_profiles(connection) -> pd.DataFrame:
    query = """
        SELECT
            u.id::text AS user_id,
            u.email,
            u.role,
            u.display_name,
            u.is_active,
            sp.survey_required,
            sp.survey_completed,
            sp.survey_type,
            sp.survey_postponed,
            sp.survey_completed_at,
            sp.age,
            sp.gender,
            sp.learner_type,
            u.created_at,
            u.updated_at
        FROM app_users u
        LEFT JOIN student_profiles sp ON sp.user_id = u.id
        WHERE u.is_active = TRUE
        ORDER BY u.created_at
    """
    frame = query_frame(connection, query)
    if frame.empty:
        return frame
    frame["user_id_hash"] = frame["user_id"].map(hash_user_id)
    frame["audience_group"] = frame["age"].map(lambda value: "school" if pd.notna(value) and int(value) <= 18 else "university" if pd.notna(value) else "unknown")
    frame["email_hash"] = frame["email"].map(hash_user_id)
    return frame.drop(columns=["email", "display_name"])


def read_chat_session_users(connection) -> pd.DataFrame:
    query = """
        SELECT
            csm.anonymous_session_id,
            csm.user_id::text AS user_id,
            csm.age,
            csm.survey_type,
            csm.user_group,
            sp.survey_completed,
            csm.created_at,
            csm.updated_at
        FROM chat_session_user_map csm
        LEFT JOIN student_profiles sp ON sp.user_id = csm.user_id
        ORDER BY csm.updated_at
    """
    frame = query_frame(connection, query)
    if frame.empty:
        return frame
    frame["user_id_hash"] = frame["user_id"].map(hash_user_id)
    frame["audience_group"] = frame["age"].map(lambda value: "school" if pd.notna(value) and int(value) <= 18 else "university" if pd.notna(value) else "unknown")
    return frame


def upload_frame(bucket, frame: pd.DataFrame, prefix: str, export_date: str, batch_id: str) -> str | None:
    if frame.empty:
        return None
    object_name = f"{prefix.strip('/')}/date={export_date}/{prefix.split('/')[-1]}_{batch_id}.parquet"
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob = bucket.blob(object_name)
    blob.upload_from_file(buffer, content_type="application/octet-stream")
    return f"gs://{bucket.name}/{object_name}"


def query_frame(connection, query: str) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = [dict(row) for row in cursor.fetchall()]
    return pd.DataFrame(rows)


def mark_survey_exported(connection, survey_response_ids: list[str], batch_id: str) -> None:
    if not survey_response_ids:
        return
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE survey_responses
            SET exported_at = NOW(),
                export_batch_id = %s,
                updated_at = NOW()
            WHERE id = ANY(%s::uuid[])
              AND exported_at IS NULL
            """,
            (batch_id, survey_response_ids),
        )
    connection.commit()


def _json_object(value) -> dict:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _mapped_survey_answers(row: pd.Series, export_date: str) -> dict:
    survey_type = row.get("survey_type")
    if survey_type not in {"school", "university"}:
        return {}
    questions = {question["id"]: question for question in load_survey_questions(survey_type)}
    answers = _json_object(row.get("answers"))
    mapped: dict[str, object] = {}
    for question_id, answer in answers.items():
        mapped[f"answer_{question_id}"] = answer
        question = questions.get(question_id)
        if question:
            mapped.update(dashboard_columns_for_answer(survey_type, question, answer))
    mapped["source_group"] = survey_type
    mapped["source_dataset"] = "app_survey_responses"
    mapped["export_date"] = export_date
    return mapped


def main() -> None:
    args = parse_args()
    batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    with connect() as connection:
        surveys = read_survey_responses(connection, True, args.export_date)
        users = read_user_profiles(connection)
        sessions = read_chat_session_users(connection)

        print(
            json.dumps(
                {
                    "batch_id": batch_id,
                    "export_date": args.export_date,
                    "survey_rows": int(len(surveys)),
                    "user_profile_rows": int(len(users)),
                    "chat_session_rows": int(len(sessions)),
                    "dry_run": args.dry_run,
                },
                sort_keys=True,
            )
        )
        if args.dry_run:
            return

        bucket = storage_bucket(args.bucket)
        survey_snapshot_uri = upload_survey_snapshot(surveys, args.bucket)
        outputs = {
            "surveys": survey_snapshot_uri,
            "user_profiles": upload_frame(bucket, users, USER_PROFILE_BRONZE_PREFIX, args.export_date, batch_id),
            "chat_sessions": upload_frame(bucket, sessions, CHAT_SESSION_BRONZE_PREFIX, args.export_date, batch_id),
        }
        if not surveys.empty:
            mark_survey_exported(connection, surveys["survey_response_id"].tolist(), batch_id)
        print(json.dumps({"batch_id": batch_id, "outputs": outputs, "status": "success"}, sort_keys=True))


def upload_survey_snapshot(frame: pd.DataFrame, bucket_name: str) -> str | None:
    if frame.empty:
        return None
    snapshot = frame.copy()
    snapshot["submitted_at"] = snapshot["created_at"].astype(str)
    snapshot["schema_version"] = SCHEMA_VERSION
    snapshot["source_dataset"] = "app_survey_snapshot"
    if "date" not in snapshot.columns:
        snapshot["date"] = snapshot["created_at"].astype(str).str.slice(0, 10)
    return replace_snapshot(snapshot.to_dict(orient="records"), bucket_name=bucket_name)


if __name__ == "__main__":
    main()
