"""Đọc một mẫu nhỏ log JSONL Bronze bằng code chạy tại local/VSCode."""

import json

from gcs_client import BUCKET_NAME, configure_console_output, list_blobs, read_jsonl_from_gcs


BRONZE_PREFIX = "bronze/"
MAX_FILES = 5
MAX_LINES_PER_FILE = 5


def main() -> None:
    configure_console_output()
    file_count = 0

    for blob in list_blobs(BRONZE_PREFIX):
        if not blob.name.lower().endswith(".jsonl"):
            continue
        if file_count >= MAX_FILES:
            break
        file_count += 1
        print(f"\nFile {file_count}: gs://{BUCKET_NAME}/{blob.name}")
        records = read_jsonl_from_gcs(blob.name, max_lines=MAX_LINES_PER_FILE)
        for line_number, record in enumerate(records, start=1):
            print(f"  Record {line_number}: {json.dumps(record, ensure_ascii=False)}")

    if file_count == 0:
        print(f"Không tìm thấy file .jsonl trong gs://{BUCKET_NAME}/{BRONZE_PREFIX}")
    else:
        print(f"\nĐã đọc sample từ {file_count} file; không ghi dữ liệu.")


if __name__ == "__main__":
    main()
