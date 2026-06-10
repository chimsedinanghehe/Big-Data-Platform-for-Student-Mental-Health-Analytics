"""Các thao tác Google Cloud Storage dành riêng cho dữ liệu pipeline local."""

import json
import sys
from typing import Any, Dict, Iterable, Iterator, List, Optional

from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"

SILVER_OUTPUT_PATH = "silver/chat_events/processed_chat_events_sample.jsonl"
GOLD_OUTPUT_PATH = "gold/dashboard_tables/hourly_chat_metrics_sample.jsonl"
ALLOWED_OUTPUT_PATHS = {SILVER_OUTPUT_PATH, GOLD_OUTPUT_PATH}


def configure_console_output() -> None:
    """Cho phép in tiếng Việt và nội dung JSON Unicode trên Windows."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def get_bucket() -> storage.Bucket:
    """Tạo bucket handle bằng Application Default Credentials trên local."""
    client = storage.Client(project=PROJECT_ID)
    return client.bucket(BUCKET_NAME)


def list_blobs(prefix: str, max_results: Optional[int] = None) -> Iterator[storage.Blob]:
    """Liệt kê object dữ liệu theo prefix, không thay đổi object trên Cloud."""
    client = storage.Client(project=PROJECT_ID)
    return client.list_blobs(
        BUCKET_NAME,
        prefix=prefix,
        max_results=max_results,
    )


def read_jsonl_from_gcs(
    path: str,
    max_lines: Optional[int] = None,
    stats: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    """Đọc JSONL từ GCS; bỏ qua dòng JSON lỗi và trả về các object hợp lệ."""
    configure_console_output()
    client = storage.Client(project=PROJECT_ID)
    blob = client.bucket(BUCKET_NAME).blob(path)
    if not blob.exists(client=client):
        print(f"Warning: Không tìm thấy file dữ liệu gs://{BUCKET_NAME}/{path}")
        return []

    rows: List[Dict[str, Any]] = []
    with blob.open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if max_lines is not None and line_number > max_lines:
                break
            if stats is not None:
                stats["raw_lines"] = stats.get("raw_lines", 0) + 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"Warning: Bỏ qua dòng {line_number} lỗi JSON trong {path} ({exc}).")
                continue
            if not isinstance(row, dict):
                print(f"Warning: Bỏ qua dòng {line_number} không phải JSON object trong {path}.")
                continue
            rows.append(row)
    return rows


def write_jsonl_to_gcs(
    path: str,
    rows: Iterable[Dict[str, Any]],
    dry_run: bool = True,
) -> bool:
    """Chỉ ghi output data Silver/Gold; dry-run không upload bất kỳ dữ liệu nào."""
    configure_console_output()
    if path not in ALLOWED_OUTPUT_PATHS:
        raise ValueError(
            "Chỉ được ghi JSONL data vào output Silver/Gold đã cấu hình; "
            f"không cho phép path: {path}"
        )

    output_rows = list(rows)
    output_uri = f"gs://{BUCKET_NAME}/{path}"
    if dry_run:
        print(f"DRY_RUN=True: sẽ ghi {len(output_rows)} dòng data vào {output_uri}; không upload.")
        return False

    payload = "".join(
        f"{json.dumps(row, ensure_ascii=False)}\n" for row in output_rows
    )
    blob = get_bucket().blob(path)
    try:
        # Không ghi đè output đã tồn tại nếu chưa được xử lý riêng.
        blob.upload_from_string(
            payload,
            content_type="application/x-ndjson",
            if_generation_match=0,
        )
    except PreconditionFailed:
        print(f"Không upload: output đã tồn tại và không được ghi đè: {output_uri}")
        return False

    print(f"Đã upload {len(output_rows)} dòng data lên {output_uri}")
    return True
