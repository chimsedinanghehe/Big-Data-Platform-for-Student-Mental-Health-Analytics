"""Pipeline local Bronze -> Silver cho sample log chatbot trên Cloud Storage."""

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from gcs_client import configure_console_output, list_blobs, read_jsonl_from_gcs, write_jsonl_to_gcs


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
BRONZE_PREFIX = "bronze/"
SILVER_OUTPUT_PATH = "silver/chat_events/processed_chat_events_sample.jsonl"
MAX_FILES = 10
MAX_LINES_PER_FILE = 1000
DRY_RUN = True

HIGH_RISK_KEYWORDS = ("kill", "suicide", "hurt someone", "harm someone", "murder")
MEDIUM_RISK_KEYWORDS = ("sad", "stress", "depress", "anxiety", "panic")
NEGATIVE_KEYWORDS = ("sad", "stress", "depress", "anxiety", "panic", "kill", "suicide", "hurt", "harm")
POSITIVE_KEYWORDS = ("happy", "good", "thanks", "thank you", "great")


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\n", " ").replace("\r", " ")
    return re.sub(r"\s+", " ", text).strip()


def parse_timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    timestamp_text = value.strip()
    if timestamp_text.endswith("Z"):
        timestamp_text = f"{timestamp_text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(timestamp_text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def classify_risk(question: str) -> str:
    lowered = question.lower()
    if any(keyword in lowered for keyword in HIGH_RISK_KEYWORDS):
        return "high"
    if any(keyword in lowered for keyword in MEDIUM_RISK_KEYWORDS):
        return "medium"
    return "low"


def classify_sentiment(question: str) -> str:
    lowered = question.lower()
    if any(keyword in lowered for keyword in NEGATIVE_KEYWORDS):
        return "negative"
    if any(keyword in lowered for keyword in POSITIVE_KEYWORDS):
        return "positive"
    return "neutral"


def classify_topic(question: str, is_document_rag: bool) -> str:
    lowered = question.lower()
    if any(keyword in lowered for keyword in ("kill", "murder", "hurt someone", "harm someone")):
        return "harm_intent"
    if any(keyword in lowered for keyword in ("suicide", "self harm")):
        return "self_harm"
    if any(keyword in lowered for keyword in MEDIUM_RISK_KEYWORDS):
        return "mental_health"
    if is_document_rag:
        return "rag_question"
    return "general"


def transform_record(source_record: Dict[str, Any], created_at: str) -> Optional[Dict[str, Any]]:
    event_id = str(source_record.get("event_id") or "").strip()
    timestamp = parse_timestamp(source_record.get("timestamp"))
    if not event_id or timestamp is None:
        return None

    question_clean = clean_text(source_record.get("question"))
    answer_clean = clean_text(source_record.get("answer"))
    standalone_query_clean = clean_text(source_record.get("standalone_query"))
    is_document_rag = as_bool(source_record.get("is_document_rag"))
    return {
        "event_id": event_id,
        "event_type": source_record.get("event_type"),
        "timestamp": timestamp.isoformat(),
        "date": timestamp.date().isoformat(),
        "hour": timestamp.hour,
        "anonymous_session_id": source_record.get("anonymous_session_id"),
        "question_clean": question_clean,
        "answer_clean": answer_clean,
        "standalone_query_clean": standalone_query_clean,
        "model": source_record.get("model"),
        "is_document_rag": is_document_rag,
        "question_length": len(question_clean),
        "answer_length": len(answer_clean),
        "risk_level": classify_risk(question_clean),
        "sentiment": classify_sentiment(question_clean),
        "topic": classify_topic(question_clean, is_document_rag),
        "is_valid": True,
        "created_at": created_at,
    }


def build_silver_sample() -> List[Dict[str, Any]]:
    output_rows: List[Dict[str, Any]] = []
    seen_event_ids: Set[str] = set()
    stats = {"files": 0, "raw_lines": 0}
    created_at = datetime.now(timezone.utc).isoformat()

    for blob in list_blobs(BRONZE_PREFIX):
        if not blob.name.lower().endswith(".jsonl"):
            continue
        if stats["files"] >= MAX_FILES:
            break
        stats["files"] += 1
        print(f"Đọc Bronze: gs://{BUCKET_NAME}/{blob.name}")

        for source_record in read_jsonl_from_gcs(blob.name, MAX_LINES_PER_FILE, stats):
            event_id = str(source_record.get("event_id") or "").strip()
            if event_id in seen_event_ids:
                continue
            transformed = transform_record(source_record, created_at)
            if transformed is None:
                continue
            seen_event_ids.add(event_id)
            output_rows.append(transformed)

    print("\nThống kê Bronze -> Silver:")
    print(f"- Số file đã đọc: {stats['files']}")
    print(f"- Số dòng raw đã đọc: {stats['raw_lines']}")
    print(f"- Số dòng hợp lệ: {len(output_rows)}")
    return output_rows


def main() -> None:
    configure_console_output()
    silver_rows = build_silver_sample()

    if DRY_RUN:
        print("\n5 record Silver đầu tiên:")
        for row in silver_rows[:5]:
            print(json.dumps(row, ensure_ascii=False))
        write_jsonl_to_gcs(SILVER_OUTPUT_PATH, silver_rows, dry_run=True)
        return

    if not silver_rows:
        print("Không có dữ liệu hợp lệ để upload Silver.")
        return
    write_jsonl_to_gcs(SILVER_OUTPUT_PATH, silver_rows, dry_run=False)


if __name__ == "__main__":
    main()
