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
    "chat_model_usage": "gold/dashboard_tables/chat_model_usage/",
    "chat_construct_summary": "gold/dashboard_tables/chat_construct_summary/",
    "chat_sentiment_summary": "gold/sentiment_summary/chat_sentiment_summary/",
}

REAL_CHAT_AUDIENCE_GROUPS = {"school", "university"}


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
        gold_path = f"gs://{BUCKET_NAME}/{prefix}"
        try:
            blob_names = list_parquet_blobs(prefix)
            if not blob_names:
                raise FileNotFoundError(f"Missing Gold Parquet files under {gold_path}")
            frame = read_parquet_blobs(blob_names)
            frame.attrs.update(
                {
                    "gold_path": gold_path,
                    "gold_prefix": prefix,
                    "load_status": "loaded",
                    "warning": "",
                    "part_count": len(blob_names),
                    "row_count": int(len(frame)),
                }
            )
            tables[table_name] = frame
        except Exception as exc:
            frame = pd.DataFrame()
            frame.attrs.update(
                {
                    "gold_path": gold_path,
                    "gold_prefix": prefix,
                    "load_status": "missing",
                    "warning": f"Bảng Gold {table_name} chưa tồn tại hoặc chưa đọc được: {exc}",
                    "part_count": 0,
                    "row_count": 0,
                }
            )
            tables[table_name] = frame
    return normalize_chat_gold_tables(tables)


def normalize_chat_gold_tables(tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    normalized = {name: frame.copy() for name, frame in tables.items()}
    hourly = normalized.get("chat_hourly_metrics", pd.DataFrame())
    if not hourly.empty:
        attrs = dict(hourly.attrs)
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
        if "date" not in hourly.columns:
            hourly["date"] = ""
        hourly["date"] = pd.to_datetime(hourly["date"], errors="coerce").dt.date.astype(str)
        hourly, audience_available = normalize_chat_audience_group(hourly)
        hourly = hourly.sort_values(["date", "hour"]).reset_index(drop=True)
        attrs["audience_group_available"] = audience_available
        hourly.attrs.update(attrs)
        normalized["chat_hourly_metrics"] = hourly

    for table_name, value_column in {
        "chat_risk_summary": "risk_level",
        "chat_topic_summary": "topic",
        "chat_construct_summary": "chat_construct",
        "chat_sentiment_summary": "sentiment",
        "chat_model_usage": "model",
    }.items():
        frame = normalized.get(table_name, pd.DataFrame())
        if frame.empty:
            continue
        attrs = dict(frame.attrs)
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
        frame, audience_available = normalize_chat_audience_group(frame)
        frame = frame.reset_index(drop=True)
        attrs["audience_group_available"] = audience_available
        frame.attrs.update(attrs)
        normalized[table_name] = frame
    return normalized


def normalize_chat_audience_group(frame: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    result = frame.copy()
    if "audience_group" not in result.columns:
        result["audience_group"] = "overall"
        return result, False

    raw = result["audience_group"].fillna("unknown").astype(str).str.strip().str.lower()
    normalized = pd.Series("unknown", index=result.index, dtype="object")
    normalized[raw.isin(["school", "student_school", "high_school"])] = "school"
    normalized[raw.isin(["university", "college", "student_university"])] = "university"
    normalized[raw.str.contains("school|hoc sinh|hocsinh", regex=True, na=False)] = "school"
    normalized[raw.str.contains("university|college|sinh vien|sinhvien", regex=True, na=False)] = "university"
    normalized[raw.isin(["overall", "all", "total"])] = "overall"
    result["audience_group"] = normalized
    return result, bool(set(normalized.dropna()) & REAL_CHAT_AUDIENCE_GROUPS)


def filter_chat_gold_tables_by_audience(
    tables: Dict[str, pd.DataFrame],
    audience_group: str | None,
) -> Dict[str, pd.DataFrame]:
    if audience_group is None:
        return {name: frame.copy() for name, frame in tables.items()}

    filtered: Dict[str, pd.DataFrame] = {}
    for table_name, frame in tables.items():
        if frame.empty:
            filtered[table_name] = frame.copy()
            continue
        attrs = dict(frame.attrs)
        if not attrs.get("audience_group_available", False) or "audience_group" not in frame.columns:
            empty = frame.iloc[0:0].copy()
            empty.attrs.update(attrs)
            filtered[table_name] = empty
            continue
        subset = frame[frame["audience_group"] == audience_group].copy()
        subset.attrs.update(attrs)
        filtered[table_name] = subset
    return filtered


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
