"""Read dashboard-ready Gold tables from Cloud Storage for Streamlit."""

from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, Iterable, List

import pandas as pd


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"

CHAT_GOLD_TABLE_PREFIXES = {
    "chat_hourly_metrics": "gold/dashboard_tables/chat_hourly_metrics/",
    "chat_risk_summary": "gold/dashboard_tables/chat_risk_summary/",
    "chat_topic_summary": "gold/dashboard_tables/chat_topic_summary/",
    "chat_construct_summary": "gold/dashboard_tables/chat_construct_summary/",
    "chat_sentiment_summary": "gold/sentiment_summary/chat_sentiment_summary/",
}

OPTIONAL_CHAT_GOLD_TABLES = {"chat_construct_summary"}


def create_storage_client():
    import google.auth
    from google.cloud import storage

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if hasattr(credentials, "with_quota_project"):
        credentials = credentials.with_quota_project(None)
    return storage.Client(project=PROJECT_ID, credentials=credentials)


def partition_values_from_blob_name(blob_name: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for segment in blob_name.split("/"):
        if "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        if key and value:
            values[key] = value
    return values


def list_parquet_blobs(prefix: str) -> List[str]:
    client = create_storage_client()
    return sorted(
        blob.name
        for blob in client.list_blobs(BUCKET_NAME, prefix=prefix)
        if blob.name.lower().endswith(".parquet") and int(blob.size or 0) > 0
    )


def read_parquet_blobs(blob_names: Iterable[str]) -> pd.DataFrame:
    client = create_storage_client()
    bucket = client.bucket(BUCKET_NAME)
    frames: List[pd.DataFrame] = []
    for blob_name in blob_names:
        frame = pd.read_parquet(BytesIO(bucket.blob(blob_name).download_as_bytes()))
        for column, value in partition_values_from_blob_name(blob_name).items():
            if column not in frame.columns:
                frame[column] = value
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def load_gold_table(prefix: str) -> pd.DataFrame:
    blob_names = list_parquet_blobs(prefix)
    if not blob_names:
        raise FileNotFoundError(f"Missing Gold Parquet files under gs://{BUCKET_NAME}/{prefix}")
    return read_parquet_blobs(blob_names)


def load_chat_gold_tables() -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}
    for table_name, prefix in CHAT_GOLD_TABLE_PREFIXES.items():
        try:
            tables[table_name] = load_gold_table(prefix)
        except FileNotFoundError:
            if table_name not in OPTIONAL_CHAT_GOLD_TABLES:
                raise
            tables[table_name] = pd.DataFrame()
    return normalize_chat_gold_tables(tables)


def normalize_chat_gold_tables(tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    normalized = {name: frame.copy() for name, frame in tables.items()}
    hourly = normalized.get("chat_hourly_metrics", pd.DataFrame())
    if not hourly.empty:
        numeric_columns = [
            "hour",
            "total_messages",
            "unique_sessions",
            "rag_messages",
            "non_rag_messages",
            "rag_rate",
            "avg_question_length",
            "avg_answer_length",
            "high_risk_count",
            "medium_risk_count",
            "low_risk_count",
            "positive_count",
            "neutral_count",
            "negative_count",
            "harm_intent_count",
            "self_harm_count",
            "mental_health_count",
            "rag_question_count",
            "general_count",
        ]
        for column in numeric_columns:
            if column not in hourly.columns:
                hourly[column] = 0
            hourly[column] = pd.to_numeric(hourly[column], errors="coerce").fillna(0)
        if "date" in hourly.columns:
            hourly["date"] = pd.to_datetime(hourly["date"], errors="coerce").dt.date.astype(str)
        normalized["chat_hourly_metrics"] = hourly.sort_values(["date", "hour"]).reset_index(drop=True)

    for table_name, value_column in {
        "chat_risk_summary": "risk_level",
        "chat_topic_summary": "topic",
        "chat_construct_summary": "chat_construct",
        "chat_sentiment_summary": "sentiment",
    }.items():
        frame = normalized.get(table_name, pd.DataFrame())
        if frame.empty:
            continue
        if "count" in frame.columns:
            frame["count"] = pd.to_numeric(frame["count"], errors="coerce").fillna(0)
        if "percentage" in frame.columns:
            frame["percentage"] = pd.to_numeric(frame["percentage"], errors="coerce").fillna(0)
        for numeric_column in [
            "high_risk_count",
            "negative_count",
            "rag_messages",
            "unique_sessions",
            "avg_question_length",
            "high_risk_rate",
            "negative_rate",
            "rag_rate",
        ]:
            if numeric_column in frame.columns:
                frame[numeric_column] = pd.to_numeric(frame[numeric_column], errors="coerce").fillna(0)
        if value_column in frame.columns:
            frame[value_column] = frame[value_column].fillna("unknown").astype(str)
        normalized[table_name] = frame.reset_index(drop=True)
    return normalized


def load_hourly_chat_metrics() -> pd.DataFrame:
    return load_chat_gold_tables()["chat_hourly_metrics"]


def get_dashboard_kpis(df: pd.DataFrame) -> Dict[str, Any]:
    empty_kpis = {
        "total_messages": 0,
        "unique_sessions": 0,
        "rag_rate": 0.0,
        "high_risk_count": 0,
        "avg_answer_length": 0.0,
    }
    if df.empty:
        return empty_kpis

    total_messages = int(df.get("total_messages", pd.Series(dtype=float)).sum())
    rag_messages = float(df.get("rag_messages", pd.Series(dtype=float)).sum())
    answer_weighted_total = (
        df["avg_answer_length"].mul(df["total_messages"]).sum()
        if {"avg_answer_length", "total_messages"}.issubset(df.columns)
        else 0.0
    )
    return {
        "total_messages": total_messages,
        "unique_sessions": int(df.get("unique_sessions", pd.Series(dtype=float)).sum()),
        "rag_rate": round(rag_messages / total_messages, 4) if total_messages else 0.0,
        "high_risk_count": int(df.get("high_risk_count", pd.Series(dtype=float)).sum()),
        "avg_answer_length": round(answer_weighted_total / total_messages, 2)
        if total_messages
        else 0.0,
    }
