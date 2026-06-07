import os
import socket

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

from backend.api.rag import router as rag_router
from backend.api.survey import router as survey_router
from backend.api.users import auth_router, users_router
from backend.db.connection import connect, get_database_url, initialize_schema_if_configured


app = FastAPI(
    title="Student Mental Health Analytics API",
    version="0.1.0",
)


def configured_cors_origins() -> tuple[list[str], str | None]:
    configured = os.getenv("CORS_ALLOWED_ORIGINS", "")
    origins = [origin.strip().rstrip("/") for origin in configured.split(",") if origin.strip()]
    if origins:
        return origins, None
    return (
        [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
        ],
        r"^http://(localhost|127\.0\.0\.1):\d+$",
    )


cors_origins, cors_origin_regex = configured_cors_origins()


app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_origin_regex=cors_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(rag_router)
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(survey_router)


@app.on_event("startup")
def initialize_database() -> None:
    try:
        initialize_schema_if_configured()
    except Exception as exc:
        print(f"User database initialization failed: {exc}")


@app.get("/health")
def health_check(response: Response):
    database = "not_configured"
    if get_database_url():
        try:
            with connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            database = "ok"
        except Exception as exc:
            response.status_code = 503
            return {"status": "degraded", "database": "error", "error": type(exc).__name__}
    return {"status": "ok", "database": database}


@app.get("/ready")
def readiness_check(response: Response):
    checks = {
        "database": _database_ready(),
        "gcs": _gcs_ready(),
        "kafka": _kafka_ready(),
    }
    ready = all(check["ok"] for check in checks.values())
    if not ready:
        response.status_code = 503
    return {"status": "ready" if ready else "not_ready", "checks": checks}


def _database_ready() -> dict[str, object]:
    if not get_database_url():
        return {"ok": False, "error": "DATABASE_URL is not configured"}
    try:
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__}


def _gcs_ready() -> dict[str, object]:
    bucket_name = os.getenv("GCS_BUCKET_NAME") or os.getenv("GCS_BUCKET")
    if not bucket_name:
        return {"ok": False, "error": "GCS bucket is not configured"}
    try:
        from google.cloud import storage

        client = storage.Client(project=os.getenv("GCP_PROJECT_ID") or None)
        client.get_bucket(bucket_name)
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__}


def _kafka_ready() -> dict[str, object]:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092").split(",")[0].strip()
    host, separator, port_value = bootstrap.rpartition(":")
    if not separator or not host:
        return {"ok": False, "error": "Invalid KAFKA_BOOTSTRAP_SERVERS"}
    try:
        with socket.create_connection((host, int(port_value)), timeout=5):
            return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__}
