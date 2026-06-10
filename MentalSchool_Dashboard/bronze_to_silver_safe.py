"""Pipeline mẫu an toàn: chuẩn hóa log hội thoại từ Bronze sang Silver."""

import json
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
BRONZE_PREFIX = "bronze/"
SILVER_OUTPUT_PATH = "silver/chat_events/processed_chat_events_sample.jsonl"
MAX_FILES = 5
MAX_LINES_PER_FILE = 100
DRY_RUN = True

HIGH_RISK_KEYWORDS = ("kill", "suicide", "hurt someone", "harm someone", "murder")
MEDIUM_RISK_KEYWORDS = ("sad", "stress", "depress", "anxiety", "panic")


def configure_console_output() -> None:
    """Cho phép in dữ liệu dry-run Unicode trên Windows."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def clean_text(value: Any) -> str:
    """Loại bỏ xuống dòng và gom khoảng trắng trong nội dung chat."""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse timestamp ISO và chuẩn hóa về UTC."""
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
    question_lower = question.lower()
    if any(keyword in question_lower for keyword in HIGH_RISK_KEYWORDS):
        return "high"
    if any(keyword in question_lower for keyword in MEDIUM_RISK_KEYWORDS):
        return "medium"
    return "low"


def build_silver_records(bucket: storage.Bucket) -> Tuple[List[Dict[str, Any]], int, int]:
    records: List[Dict[str, Any]] = []
    seen_event_ids: Set[str] = set()
    created_at = datetime.now(timezone.utc).isoformat()
    jsonl_file_count = 0
    raw_line_count = 0

    for blob in bucket.list_blobs(prefix=BRONZE_PREFIX):
        if not blob.name.lower().endswith(".jsonl"):
            continue
        if jsonl_file_count >= MAX_FILES:
            break
        jsonl_file_count += 1
        print(f"Đọc sample Bronze: {blob.name}")

        with blob.open("r", encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if line_number > MAX_LINES_PER_FILE:
                    break
                raw_line_count += 1
                try:
                    source_record = json.loads(line)
                except json.JSONDecodeError as exc:
                    print(f"  Bỏ dòng {line_number}: lỗi JSON ({exc}).")
                    continue
                if not isinstance(source_record, dict):
                    print(f"  Bỏ dòng {line_number}: JSON không phải object.")
                    continue

                event_id = str(source_record.get("event_id") or "").strip()
                parsed_timestamp = parse_timestamp(source_record.get("timestamp"))
                if not event_id or parsed_timestamp is None:
                    print(f"  Bỏ dòng {line_number}: thiếu event_id hoặc timestamp hợp lệ.")
                    continue
                if event_id in seen_event_ids:
                    print(f"  Bỏ dòng {line_number}: trùng event_id {event_id}.")
                    continue
                seen_event_ids.add(event_id)

                question_clean = clean_text(source_record.get("question"))
                answer_clean = clean_text(source_record.get("answer"))
                standalone_query_clean = clean_text(source_record.get("standalone_query"))

                records.append(
                    {
                        "event_id": event_id,
                        "event_type": source_record.get("event_type"),
                        "timestamp": parsed_timestamp.isoformat(),
                        "date": parsed_timestamp.date().isoformat(),
                        "hour": parsed_timestamp.hour,
                        "anonymous_session_id": source_record.get("anonymous_session_id"),
                        "question_clean": question_clean,
                        "answer_clean": answer_clean,
                        "standalone_query_clean": standalone_query_clean,
                        "model": source_record.get("model"),
                        "is_document_rag": as_bool(source_record.get("is_document_rag")),
                        "question_length": len(question_clean),
                        "answer_length": len(answer_clean),
                        "risk_level": classify_risk(question_clean),
                        "is_valid": True,
                        "created_at": created_at,
                    }
                )

    if jsonl_file_count == 0:
        print(f"Không tìm thấy file .jsonl dưới prefix {BRONZE_PREFIX}")
    return records, jsonl_file_count, raw_line_count


def upload_silver_sample(bucket: storage.Bucket, records: List[Dict[str, Any]]) -> None:
    payload = "".join(
        f"{json.dumps(record, ensure_ascii=False)}\n" for record in records
    )
    blob = bucket.blob(SILVER_OUTPUT_PATH)
    output_uri = f"gs://{BUCKET_NAME}/{SILVER_OUTPUT_PATH}"
    try:
        # Điều kiện generation=0 ngăn việc ghi đè object Silver đã có.
        blob.upload_from_string(
            payload,
            content_type="application/x-ndjson",
            if_generation_match=0,
        )
    except PreconditionFailed:
        print("Không upload Silver: object đích đã tồn tại và pipeline không ghi đè.")
        print(output_uri)
        return
    print(f"Đã upload Silver sample: {output_uri}")


def main() -> None:
    configure_console_output()
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    silver_records, file_count, raw_line_count = build_silver_records(bucket)

    print("\nThống kê sample Bronze -> Silver:")
    print(f"- Số file JSONL đã đọc: {file_count}")
    print(f"- Số dòng raw đã đọc: {raw_line_count}")
    print(f"- Số event Silver hợp lệ: {len(silver_records)}")

    if DRY_RUN:
        print("DRY_RUN=True: in tối đa 5 record Silver, không upload lên Cloud Storage.")
        for record in silver_records[:5]:
            print(json.dumps(record, ensure_ascii=False))
        return

    if not silver_records:
        print("Không có record để upload.")
        return

    upload_silver_sample(bucket, silver_records)


if __name__ == "__main__":
    main()
