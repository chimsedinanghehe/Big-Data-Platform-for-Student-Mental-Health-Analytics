from __future__ import annotations

import os
from datetime import datetime, timezone
from io import BytesIO
from uuid import uuid4

import pandas as pd
from dotenv import load_dotenv

from backend.chat_logs.gcs_writer import hash_user_id


load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or "student-mental-health-496205"
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME") or os.getenv("GCS_BUCKET") or "student-mental-health-lake-nhom1-2026"
SNAPSHOT_PREFIX = "bronze/app_survey_snapshot"
SNAPSHOT_OBJECT = f"{SNAPSHOT_PREFIX}/survey_all.parquet"
SCHEMA_VERSION = "survey_snapshot_v1"
MINIMUM_COLUMNS = {"user_id", "age", "survey_type", "submitted_at", "schema_version"}
SNAPSHOT_METADATA_COLUMNS = {
    "survey_response_id",
    "user_id",
    "user_id_hash",
    "age",
    "survey_type",
    "submitted_at",
    "date",
    "gender",
    "learner_type",
    "source_group",
    "source_dataset",
    "source_layer",
    "schema_version",
}


def build_survey_snapshot_record(
    *,
    survey_response_id: str,
    user_id: str,
    survey_type: str,
    age: int | None,
    gender: str | None,
    learner_type: str | None,
    submitted_at: datetime | None,
    normalized_answers: dict[str, object],
) -> dict[str, object]:
    submitted = submitted_at or datetime.now(timezone.utc)
    if submitted.tzinfo is None:
        submitted = submitted.replace(tzinfo=timezone.utc)
    record = {
        "survey_response_id": survey_response_id,
        "user_id": user_id,
        "user_id_hash": hash_user_id(user_id),
        "age": age,
        "survey_type": survey_type,
        "submitted_at": submitted.isoformat(),
        "date": submitted.date().isoformat(),
        "gender": gender,
        "learner_type": learner_type,
        "source_group": survey_type,
        "source_dataset": "app_survey_snapshot",
        "source_layer": "bronze",
        "schema_version": SCHEMA_VERSION,
    }
    for key, value in normalized_answers.items():
        if key in SNAPSHOT_METADATA_COLUMNS:
            record[f"survey_answer_{key}"] = value
        else:
            record[key] = value
    return record


def merge_record_into_snapshot(record: dict[str, object], *, bucket_name: str = BUCKET_NAME) -> str:
    """Safely merge one survey row into the GCS snapshot.

    Callers must hold the single-writer lock before invoking this function.
    The function writes and validates a temporary Parquet object first; the
    existing snapshot is left untouched unless validation succeeds.
    """
    bucket = _storage_bucket(bucket_name)
    current = _read_snapshot(bucket)
    merged = _merge_unique_by_user_id(current, pd.DataFrame([record]))
    _validate_snapshot(merged)

    tmp_object = f"{SNAPSHOT_PREFIX}/_tmp/survey_all_tmp_{uuid4().hex}.parquet"
    payload = _frame_to_parquet_bytes(merged)
    tmp_blob = bucket.blob(tmp_object)
    tmp_blob.upload_from_string(payload, content_type="application/octet-stream")

    try:
        validated = pd.read_parquet(BytesIO(tmp_blob.download_as_bytes()))
        _validate_snapshot(validated)
        bucket.blob(SNAPSHOT_OBJECT).upload_from_string(payload, content_type="application/octet-stream")
    finally:
        tmp_blob.delete()

    return f"gs://{bucket.name}/{SNAPSHOT_OBJECT}"


def merge_records_into_snapshot(records: list[dict[str, object]], *, bucket_name: str = BUCKET_NAME) -> str:
    if not records:
        return f"gs://{bucket_name}/{SNAPSHOT_OBJECT}"
    bucket = _storage_bucket(bucket_name)
    current = _read_snapshot(bucket)
    merged = _merge_unique_by_user_id(current, pd.DataFrame(records))
    _validate_snapshot(merged)

    tmp_object = f"{SNAPSHOT_PREFIX}/_tmp/survey_all_tmp_{uuid4().hex}.parquet"
    payload = _frame_to_parquet_bytes(merged)
    tmp_blob = bucket.blob(tmp_object)
    tmp_blob.upload_from_string(payload, content_type="application/octet-stream")
    try:
        validated = pd.read_parquet(BytesIO(tmp_blob.download_as_bytes()))
        _validate_snapshot(validated)
        bucket.blob(SNAPSHOT_OBJECT).upload_from_string(payload, content_type="application/octet-stream")
    finally:
        tmp_blob.delete()
    return f"gs://{bucket.name}/{SNAPSHOT_OBJECT}"


def replace_snapshot(records: list[dict[str, object]], *, bucket_name: str = BUCKET_NAME) -> str:
    bucket = _storage_bucket(bucket_name)
    frame = _merge_unique_by_user_id(pd.DataFrame(), pd.DataFrame(records))
    _validate_snapshot(frame)

    tmp_object = f"{SNAPSHOT_PREFIX}/_tmp/survey_all_rebuild_tmp_{uuid4().hex}.parquet"
    payload = _frame_to_parquet_bytes(frame)
    tmp_blob = bucket.blob(tmp_object)
    tmp_blob.upload_from_string(payload, content_type="application/octet-stream")
    try:
        validated = pd.read_parquet(BytesIO(tmp_blob.download_as_bytes()))
        _validate_snapshot(validated)
        bucket.blob(SNAPSHOT_OBJECT).upload_from_string(payload, content_type="application/octet-stream")
    finally:
        tmp_blob.delete()
    return f"gs://{bucket.name}/{SNAPSHOT_OBJECT}"


def _storage_bucket(bucket_name: str):
    import google.auth
    from google.cloud import storage

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if hasattr(credentials, "with_quota_project"):
        credentials = credentials.with_quota_project(None)
    client = storage.Client(project=PROJECT_ID, credentials=credentials)
    return client.bucket(bucket_name)


def _read_snapshot(bucket) -> pd.DataFrame:
    blob = bucket.blob(SNAPSHOT_OBJECT)
    if not blob.exists():
        return pd.DataFrame()
    payload = blob.download_as_bytes()
    if not payload:
        return pd.DataFrame()
    return pd.read_parquet(BytesIO(payload))


def _merge_unique_by_user_id(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        merged = incoming.copy()
    else:
        merged = pd.concat([existing, incoming], ignore_index=True, sort=False)
    merged["user_id"] = merged["user_id"].astype(str)
    merged = merged.drop_duplicates(subset=["user_id"], keep="first")
    return _normalize_for_parquet(merged)


def _normalize_for_parquet(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in MINIMUM_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    for column in out.columns:
        if out[column].map(lambda value: isinstance(value, (dict, list, tuple, set))).any():
            out[column] = out[column].map(lambda value: str(value) if value is not None else None)
    for column in ["user_id", "survey_type", "submitted_at", "schema_version", "date"]:
        if column in out.columns:
            out[column] = out[column].astype("string")
    if "age" in out.columns:
        out["age"] = pd.to_numeric(out["age"], errors="coerce").astype("Int64")
    return out


def _validate_snapshot(frame: pd.DataFrame) -> None:
    missing = sorted(MINIMUM_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"Survey snapshot is missing required columns: {missing}")
    if frame["user_id"].isna().any() or frame["user_id"].astype(str).str.strip().eq("").any():
        raise ValueError("Survey snapshot contains empty user_id values.")
    duplicates = frame["user_id"].astype(str).duplicated()
    if duplicates.any():
        duplicate_ids = frame.loc[duplicates, "user_id"].astype(str).head(10).tolist()
        raise ValueError(f"Survey snapshot contains duplicate user_id values: {duplicate_ids}")
    if not bool(frame["survey_type"].dropna().astype(str).isin(["school", "university"]).all()):
        raise ValueError("Survey snapshot contains invalid survey_type values.")


def _frame_to_parquet_bytes(frame: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()
