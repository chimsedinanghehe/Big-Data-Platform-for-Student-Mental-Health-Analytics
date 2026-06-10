from __future__ import annotations

import json
from datetime import datetime
from typing import Iterable

import pandas as pd

from backend.db.connection import connect
from backend.surveys.questions import validate_and_normalize_answers
from backend.surveys.snapshot import build_survey_snapshot_record, merge_records_into_snapshot


def export_pending_survey_responses(*, limit: int = 100) -> dict[str, object]:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(hashtext('survey_snapshot_writer'))")
            try:
                rows = _read_pending_rows(cursor, limit=limit)
                return _export_rows(connection, cursor, rows)
            finally:
                cursor.execute("SELECT pg_advisory_unlock(hashtext('survey_snapshot_writer'))")


def export_survey_response_ids(survey_response_ids: Iterable[str]) -> dict[str, object]:
    ids = [str(value) for value in survey_response_ids if value]
    if not ids:
        return {"input_rows": 0, "exported_rows": 0, "snapshot_uri": None}
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(hashtext('survey_snapshot_writer'))")
            try:
                rows = _read_rows_by_ids(cursor, ids)
                return _export_rows(connection, cursor, rows)
            finally:
                cursor.execute("SELECT pg_advisory_unlock(hashtext('survey_snapshot_writer'))")


def _export_rows(connection, cursor, rows: list[dict]) -> dict[str, object]:
    records = []
    exported_ids = []
    skipped_ids = []
    for row in rows:
        answers = _json_object(row.get("answers"))
        try:
            _cleaned, normalized = validate_and_normalize_answers(survey_type=row["survey_type"], answers=answers)
        except ValueError:
            skipped_ids.append(row["survey_response_id"])
            continue
        records.append(
            build_survey_snapshot_record(
                survey_response_id=row["survey_response_id"],
                user_id=row["user_id"],
                survey_type=row["survey_type"],
                age=row.get("age"),
                gender=row.get("gender"),
                learner_type=row.get("learner_type"),
                submitted_at=row.get("created_at") or datetime.utcnow(),
                normalized_answers=normalized,
            )
        )
        exported_ids.append(row["survey_response_id"])

    snapshot_uri = None
    if records:
        snapshot_uri = merge_records_into_snapshot(records)
        _mark_exported(cursor, exported_ids, snapshot_uri)
        connection.commit()
    else:
        connection.rollback()

    return {
        "input_rows": len(rows),
        "exported_rows": len(exported_ids),
        "skipped_rows": len(skipped_ids),
        "skipped_ids": skipped_ids[:20],
        "snapshot_uri": snapshot_uri,
    }


def _read_pending_rows(cursor, *, limit: int) -> list[dict]:
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
        WHERE sr.exported_at IS NULL
        ORDER BY sr.created_at
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _read_rows_by_ids(cursor, ids: list[str]) -> list[dict]:
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
        WHERE sr.id = ANY(%s::uuid[])
        ORDER BY sr.created_at
        """,
        (ids,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _mark_exported(cursor, survey_response_ids: list[str], snapshot_uri: str) -> None:
    cursor.execute(
        """
        UPDATE survey_responses
        SET exported_at = NOW(),
            export_batch_id = %s,
            export_object_uri = %s,
            updated_at = NOW()
        WHERE id = ANY(%s::uuid[])
        """,
        ("survey_snapshot_worker_v1", snapshot_uri, survey_response_ids),
    )


def _json_object(value) -> dict:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)
