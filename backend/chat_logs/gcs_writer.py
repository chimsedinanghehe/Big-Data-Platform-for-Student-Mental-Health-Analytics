from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import re
import shutil
import subprocess
from uuid import uuid4

from backend.rag.config import RAGSettings, get_settings


EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"(?<!\d)(?:\+?\d[\d .()\-]{7,}\d)(?!\d)")
STUDENT_ID_RE = re.compile(r"\b(?:student\s*id|student_id|sid)\s*[:#-]?\s*[A-Z0-9-]{4,}\b", re.IGNORECASE)


def write_chat_turn(
    *,
    session_id: str,
    question: str,
    answer: str,
    is_document_rag: bool,
    model: str,
    standalone_query: str | None = None,
    emotion: dict | None = None,
    safety: dict | None = None,
    settings: RAGSettings | None = None,
) -> str:
    settings = settings or get_settings()
    if not settings.gcs_bucket_name:
        raise RuntimeError("GCS_BUCKET_NAME is required for chat log storage.")

    now = datetime.now(UTC)
    anonymous_session_id = anonymize_session_id(session_id)
    object_name = _object_name(
        prefix=settings.gcs_chatlog_prefix,
        date_value=now.date().isoformat(),
        anonymous_session_id=anonymous_session_id,
    )
    event = {
        "event_id": str(uuid4()),
        "event_type": "rag_chat_turn",
        "timestamp": now.isoformat(),
        "anonymous_session_id": anonymous_session_id,
        "question": mask_pii(question),
        "answer": mask_pii(answer),
        "standalone_query": mask_pii(standalone_query) if standalone_query else None,
        "model": model,
        "is_document_rag": is_document_rag,
        "emotion": emotion or {},
        "safety": safety or {},
    }

    _upload_jsonl(
        bucket_name=settings.gcs_bucket_name,
        object_name=object_name,
        event=event,
    )
    return f"gs://{settings.gcs_bucket_name}/{object_name}"


def anonymize_session_id(session_id: str) -> str:
    normalized = (session_id or "").strip()
    if not normalized:
        normalized = str(uuid4())
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"session_{digest[:32]}"


def mask_pii(text: str | None) -> str | None:
    if text is None:
        return None
    masked = EMAIL_RE.sub("[EMAIL]", text)
    masked = PHONE_RE.sub("[PHONE]", masked)
    masked = STUDENT_ID_RE.sub("[STUDENT_ID]", masked)
    return masked


def _object_name(prefix: str, date_value: str, anonymous_session_id: str) -> str:
    normalized_prefix = (prefix or "bronze/chat_logs").strip("/")
    return f"{normalized_prefix}/date={date_value}/{anonymous_session_id}_{uuid4().hex}.jsonl"


def _upload_jsonl(bucket_name: str, object_name: str, event: dict) -> None:
    payload = json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"

    try:
        _upload_jsonl_with_client(
            bucket_name=bucket_name,
            object_name=object_name,
            payload=payload,
        )
    except Exception:
        _upload_jsonl_with_gsutil(
            bucket_name=bucket_name,
            object_name=object_name,
            payload=payload,
        )


def _upload_jsonl_with_client(bucket_name: str, object_name: str, payload: str) -> None:
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(
        payload,
        content_type="application/jsonl",
    )


def _upload_jsonl_with_gsutil(bucket_name: str, object_name: str, payload: str) -> None:
    gsutil = shutil.which("gsutil.cmd") or shutil.which("gsutil")
    if not gsutil:
        raise RuntimeError("Chat log upload failed and gsutil was not found on PATH.")

    target_uri = f"gs://{bucket_name}/{object_name}"
    subprocess.run(
        [gsutil, "cp", "-", target_uri],
        input=payload.encode("utf-8"),
        check=True,
        capture_output=True,
    )
