"""Ghi một object kiểm tra vào vùng test riêng trên Google Cloud Storage."""

import sys

from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
TEST_OBJECT_PATH = "silver/test_from_vscode/test.jsonl"
TEST_CONTENT = '{"source": "vscode", "test": true}\n'


def configure_console_output() -> None:
    """Cho phép in tiếng Việt trên Windows."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def main() -> None:
    configure_console_output()
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(TEST_OBJECT_PATH)
    gcs_uri = f"gs://{BUCKET_NAME}/{TEST_OBJECT_PATH}"

    try:
        # Chỉ tạo object test mới; không ghi đè nếu object đã tồn tại.
        blob.upload_from_string(
            TEST_CONTENT,
            content_type="application/x-ndjson",
            if_generation_match=0,
        )
    except PreconditionFailed:
        print("Không upload: object test đã tồn tại và script không ghi đè.")
        print(gcs_uri)
        return

    print("Upload file test thành công:")
    print(gcs_uri)


if __name__ == "__main__":
    main()
