"""Kiểm tra kết nối đọc Google Cloud Storage từ máy local."""

import sys

from google.cloud import storage


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
MAX_OBJECTS = 30


def configure_console_output() -> None:
    """Cho phép in tiếng Việt và nội dung chat Unicode trên Windows."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def main() -> None:
    configure_console_output()
    # Dùng Application Default Credentials đã đăng nhập bằng gcloud.
    client = storage.Client(project=PROJECT_ID)
    blobs = client.list_blobs(BUCKET_NAME, max_results=MAX_OBJECTS)

    print(f"Đang liệt kê tối đa {MAX_OBJECTS} object trong gs://{BUCKET_NAME}/")
    object_count = 0
    for object_count, blob in enumerate(blobs, start=1):
        # Tên object cũng thể hiện các prefix/folder ảo trên GCS.
        print(f"{object_count:02d}. {blob.name}")

    if object_count == 0:
        print("Kết nối thành công, nhưng bucket không có object trong phạm vi liệt kê.")
    else:
        print(f"Kết nối thành công. Đã hiển thị {object_count} object.")


if __name__ == "__main__":
    main()
