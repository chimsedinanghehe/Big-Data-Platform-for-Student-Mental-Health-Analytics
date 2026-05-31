"""Pipeline mẫu an toàn: tổng hợp dữ liệu Silver thành metrics Gold."""

import json
import sys
from typing import Any, Dict, List

import pandas as pd
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
SILVER_INPUT_PATH = "silver/chat_events/processed_chat_events_sample.jsonl"
GOLD_OUTPUT_PATH = "gold/dashboard_tables/hourly_chat_metrics_sample.jsonl"
DRY_RUN = True


def configure_console_output() -> None:
    """Cho phép in metrics và thông báo tiếng Việt trên Windows."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def read_silver_records(blob: storage.Blob) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with blob.open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"Bỏ dòng Silver {line_number}: lỗi JSON ({exc}).")
                continue
            if not isinstance(record, dict):
                print(f"Bỏ dòng Silver {line_number}: JSON không phải object.")
                continue
            records.append(record)
    return records


def aggregate_hourly_metrics(records: List[Dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(records)
    required_columns = {
        "date",
        "hour",
        "anonymous_session_id",
        "is_document_rag",
        "question_length",
        "answer_length",
        "risk_level",
    }
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        print(f"Thiếu cột Silver cần thiết: {', '.join(missing_columns)}")
        return pd.DataFrame()

    frame = frame.dropna(subset=["date", "hour"]).copy()
    if frame.empty:
        return pd.DataFrame()

    frame["anonymous_session_id"] = frame["anonymous_session_id"].fillna("")
    frame["rag_flag"] = frame["is_document_rag"].map(as_bool).astype(int)
    frame["question_length"] = pd.to_numeric(frame["question_length"], errors="coerce").fillna(0)
    frame["answer_length"] = pd.to_numeric(frame["answer_length"], errors="coerce").fillna(0)
    frame["risk_level"] = frame["risk_level"].fillna("low").astype(str).str.lower()

    metrics = (
        frame.groupby(["date", "hour"], as_index=False)
        .agg(
            total_messages=("date", "size"),
            unique_sessions=("anonymous_session_id", lambda values: values[values != ""].nunique()),
            rag_messages=("rag_flag", "sum"),
            avg_question_length=("question_length", "mean"),
            avg_answer_length=("answer_length", "mean"),
            high_risk_count=("risk_level", lambda values: (values == "high").sum()),
            medium_risk_count=("risk_level", lambda values: (values == "medium").sum()),
            low_risk_count=("risk_level", lambda values: (values == "low").sum()),
        )
        .sort_values(["date", "hour"])
        .reset_index(drop=True)
    )
    metrics["non_rag_messages"] = metrics["total_messages"] - metrics["rag_messages"]
    metrics["rag_rate"] = (metrics["rag_messages"] / metrics["total_messages"]).round(4)
    metrics["avg_question_length"] = metrics["avg_question_length"].round(2)
    metrics["avg_answer_length"] = metrics["avg_answer_length"].round(2)

    count_columns = [
        "total_messages",
        "unique_sessions",
        "rag_messages",
        "non_rag_messages",
        "high_risk_count",
        "medium_risk_count",
        "low_risk_count",
    ]
    metrics[count_columns] = metrics[count_columns].astype(int)
    return metrics[
        [
            "date",
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
        ]
    ]


def upload_gold_sample(bucket: storage.Bucket, metrics: pd.DataFrame) -> None:
    payload = metrics.to_json(orient="records", lines=True, force_ascii=False)
    if payload and not payload.endswith("\n"):
        payload += "\n"
    blob = bucket.blob(GOLD_OUTPUT_PATH)
    output_uri = f"gs://{BUCKET_NAME}/{GOLD_OUTPUT_PATH}"
    try:
        # Không ghi đè bảng Gold sample nếu object đã tồn tại.
        blob.upload_from_string(
            payload,
            content_type="application/x-ndjson",
            if_generation_match=0,
        )
    except PreconditionFailed:
        print("Không upload Gold: object đích đã tồn tại và pipeline không ghi đè.")
        print(output_uri)
        return
    print(f"Đã upload Gold sample: {output_uri}")


def main() -> None:
    configure_console_output()
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    silver_blob = bucket.blob(SILVER_INPUT_PATH)
    silver_uri = f"gs://{BUCKET_NAME}/{SILVER_INPUT_PATH}"

    if not silver_blob.exists(client=client):
        print(f"Chưa tìm thấy Silver sample: {silver_uri}")
        print("Hãy chạy bronze_to_silver_safe.py với DRY_RUN=False trước để tạo Silver sample.")
        return

    silver_records = read_silver_records(silver_blob)
    metrics = aggregate_hourly_metrics(silver_records)
    if metrics.empty:
        print("Không có dữ liệu Silver hợp lệ để tổng hợp Gold.")
        return

    if DRY_RUN:
        print("DRY_RUN=True: chỉ in bảng Gold, không upload lên Cloud Storage.")
        print(metrics.to_string(index=False))
        return

    upload_gold_sample(bucket, metrics)


if __name__ == "__main__":
    main()
