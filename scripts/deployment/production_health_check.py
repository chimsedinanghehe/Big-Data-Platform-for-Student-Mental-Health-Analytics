from __future__ import annotations

import json
import os
import socket
import sys
import urllib.request
from datetime import UTC, datetime

from google.cloud import storage


PROJECT_ID = os.getenv("GCP_PROJECT_ID", "student-mental-health-496205")
BUCKET = os.getenv("GCS_BUCKET_NAME") or os.getenv(
    "GCS_BUCKET",
    "student-mental-health-lake-nhom1-2026",
)


def http_check(url: str, *, timeout: int = 20) -> dict[str, object]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return {"ok": 200 <= response.status < 400, "status": response.status}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def tcp_check(host: str, port: int) -> dict[str, object]:
    try:
        with socket.create_connection((host, port), timeout=5):
            return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def gcs_latest(prefix: str) -> dict[str, object]:
    try:
        client = storage.Client(project=PROJECT_ID)
        blobs = list(client.list_blobs(BUCKET, prefix=prefix))
        latest = max(blobs, key=lambda blob: blob.updated) if blobs else None
        age_seconds = (
            round((datetime.now(UTC) - latest.updated).total_seconds(), 1)
            if latest
            else None
        )
        return {
            "ok": latest is not None,
            "objects": len(blobs),
            "latest_object": latest.name if latest else None,
            "latest_updated": latest.updated.isoformat() if latest else None,
            "latest_age_seconds": age_seconds,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def main() -> int:
    checks = {
        "backend_health": http_check("http://127.0.0.1:8000/health", timeout=25),
        "backend_readiness": http_check("http://127.0.0.1:8000/ready", timeout=30),
        "frontend": http_check("http://127.0.0.1:5173"),
        "dashboard": http_check("http://127.0.0.1:8501"),
        "kafka_tunnel": tcp_check("127.0.0.1", 9092),
        "survey_bronze": gcs_latest("bronze/app_survey_snapshot/"),
        "chat_bronze": gcs_latest("bronze/chat_logs/"),
        "survey_silver": gcs_latest("silver/survey_cleaned/"),
        "chat_silver": gcs_latest("silver/anonymized_chat/"),
        "survey_gold": gcs_latest("gold/dashboard_tables/survey_overview_summary/"),
        "chat_gold": gcs_latest("gold/dashboard_tables/chat_hourly_metrics/"),
    }
    payload = {
        "checked_at": datetime.now(UTC).isoformat(),
        "status": "healthy" if all(item.get("ok") for item in checks.values()) else "degraded",
        "checks": checks,
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "healthy" else 1


if __name__ == "__main__":
    raise SystemExit(main())
