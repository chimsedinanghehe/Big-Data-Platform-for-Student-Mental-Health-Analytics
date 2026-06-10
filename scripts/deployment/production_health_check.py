from __future__ import annotations

import json
import os
import socket
import sys
import urllib.request
from datetime import UTC, datetime

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "student-mental-health-496205")
BUCKET = os.getenv("GCS_BUCKET_NAME") or os.getenv(
    "GCS_BUCKET",
    "student-mental-health-lake-nhom1-2026",
)
BACKEND_URL = os.getenv("HEALTH_BACKEND_URL", "http://127.0.0.1:18000")
FRONTEND_URL = os.getenv("HEALTH_FRONTEND_URL", "http://127.0.0.1:18080/health")
DASHBOARD_URL = os.getenv(
    "HEALTH_DASHBOARD_URL",
    "http://127.0.0.1:18501/dashboard/_stcore/health",
)
KAFKA_HOST = os.getenv("KAFKA_HEALTH_HOST", "127.0.0.1")
KAFKA_PORT = int(os.getenv("KAFKA_LOCAL_PORT", "9092"))


def env_enabled(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def skipped(reason: str) -> dict[str, object]:
    return {"ok": True, "skipped": True, "reason": reason}


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
        from google.cloud import storage

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
    bigdata_enabled = env_enabled("BIGDATA_ENABLED")
    checks = {
        "backend_health": http_check(f"{BACKEND_URL}/health", timeout=25),
        "backend_readiness": http_check(f"{BACKEND_URL}/ready", timeout=30),
        "frontend": http_check(FRONTEND_URL),
        "dashboard": http_check(DASHBOARD_URL),
    }

    bigdata_checks = {
        "kafka_tunnel": lambda: tcp_check(KAFKA_HOST, KAFKA_PORT),
        "survey_bronze": lambda: gcs_latest("bronze/app_survey_snapshot/"),
        "chat_bronze": lambda: gcs_latest("bronze/chat_logs/"),
        "survey_silver": lambda: gcs_latest("silver/survey_cleaned/"),
        "chat_silver": lambda: gcs_latest("silver/anonymized_chat/"),
        "survey_gold": lambda: gcs_latest("gold/dashboard_tables/survey_overview_summary/"),
        "chat_gold": lambda: gcs_latest("gold/dashboard_tables/chat_hourly_metrics/"),
    }
    if bigdata_enabled:
        checks.update({name: check() for name, check in bigdata_checks.items()})
    else:
        checks.update(
            {
                name: skipped("BIGDATA_ENABLED is false")
                for name in bigdata_checks
            }
        )

    payload = {
        "checked_at": datetime.now(UTC).isoformat(),
        "bigdata_enabled": bigdata_enabled,
        "status": "healthy" if all(item.get("ok") for item in checks.values()) else "degraded",
        "checks": checks,
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "healthy" else 1


if __name__ == "__main__":
    raise SystemExit(main())
