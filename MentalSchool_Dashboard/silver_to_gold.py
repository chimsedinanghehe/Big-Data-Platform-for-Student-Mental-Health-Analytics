"""Pipeline local Silver -> Gold cho metrics dashboard log chatbot."""

from typing import Any, Dict, List

import pandas as pd

from gcs_client import configure_console_output, read_jsonl_from_gcs, write_jsonl_to_gcs


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
SILVER_INPUT_PATH = "silver/chat_events/processed_chat_events_sample.jsonl"
GOLD_OUTPUT_PATH = "gold/dashboard_tables/hourly_chat_metrics_sample.jsonl"
DRY_RUN = True


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def aggregate_hourly_metrics(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    required = {
        "date",
        "hour",
        "anonymous_session_id",
        "is_document_rag",
        "question_length",
        "answer_length",
        "risk_level",
        "sentiment",
        "topic",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        print(f"Silver thiếu cột cần thiết: {', '.join(missing)}")
        return pd.DataFrame()

    frame = frame.dropna(subset=["date", "hour"]).copy()
    if frame.empty:
        return pd.DataFrame()

    frame["anonymous_session_id"] = frame["anonymous_session_id"].fillna("")
    frame["rag_flag"] = frame["is_document_rag"].map(as_bool).astype(int)
    frame["question_length"] = pd.to_numeric(frame["question_length"], errors="coerce").fillna(0)
    frame["answer_length"] = pd.to_numeric(frame["answer_length"], errors="coerce").fillna(0)
    frame["risk_level"] = frame["risk_level"].fillna("low").astype(str).str.lower()
    frame["sentiment"] = frame["sentiment"].fillna("neutral").astype(str).str.lower()
    frame["topic"] = frame["topic"].fillna("general").astype(str).str.lower()

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
            positive_count=("sentiment", lambda values: (values == "positive").sum()),
            neutral_count=("sentiment", lambda values: (values == "neutral").sum()),
            negative_count=("sentiment", lambda values: (values == "negative").sum()),
            harm_intent_count=("topic", lambda values: (values == "harm_intent").sum()),
            self_harm_count=("topic", lambda values: (values == "self_harm").sum()),
            mental_health_count=("topic", lambda values: (values == "mental_health").sum()),
            general_count=("topic", lambda values: (values == "general").sum()),
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
        "positive_count",
        "neutral_count",
        "negative_count",
        "harm_intent_count",
        "self_harm_count",
        "mental_health_count",
        "general_count",
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
            "positive_count",
            "neutral_count",
            "negative_count",
            "harm_intent_count",
            "self_harm_count",
            "mental_health_count",
            "general_count",
        ]
    ]


def main() -> None:
    configure_console_output()
    silver_rows = read_jsonl_from_gcs(SILVER_INPUT_PATH)
    if not silver_rows:
        print("Hãy chạy bronze_to_silver.py với DRY_RUN=False trước để tạo Silver sample.")
        return

    metrics = aggregate_hourly_metrics(silver_rows)
    if metrics.empty:
        print("Không có dữ liệu Silver hợp lệ để tổng hợp Gold.")
        return

    if DRY_RUN:
        print("DRY_RUN=True: chỉ in bảng Gold ra console, không upload.")
        print(metrics.to_string(index=False))
        write_jsonl_to_gcs(GOLD_OUTPUT_PATH, metrics.to_dict(orient="records"), dry_run=True)
        return

    write_jsonl_to_gcs(GOLD_OUTPUT_PATH, metrics.to_dict(orient="records"), dry_run=False)


if __name__ == "__main__":
    main()
