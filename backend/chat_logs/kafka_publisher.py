from __future__ import annotations

import json
import os
<<<<<<< HEAD
from datetime import datetime, UTC
from pathlib import Path
=======
from datetime import UTC, datetime
>>>>>>> a737070ebdd229bed647412b6b52a70a9aba65ba
from uuid import uuid4

from confluent_kafka import Producer
<<<<<<< HEAD
from dotenv import load_dotenv
from backend.chat_logs.gcs_writer import anonymize_session_id, mask_pii

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(BACKEND_ROOT / ".env", override=True)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "student-chat-logs")

# Cấu hình kết nối linh hoạt giữa Local và Confluent Cloud
=======

from backend.chat_logs.gcs_writer import anonymize_session_id, hash_user_id, mask_pii
from backend.surveys.questions import audience_group_for_age


CHAT_TOPIC = os.getenv("CHAT_KAFKA_TOPIC", "student-chat-logs")

>>>>>>> a737070ebdd229bed647412b6b52a70a9aba65ba
kafka_config = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    "enable.idempotence": True,
    "acks": "all",
    "retries": 10,
    "retry.backoff.ms": 500,
    "request.timeout.ms": 10000,
    "delivery.timeout.ms": 30000,
    "socket.keepalive.enable": True,
}
if os.getenv("KAFKA_SASL_USERNAME"):
    kafka_config.update(
        {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.getenv("KAFKA_SASL_USERNAME"),
            "sasl.password": os.getenv("KAFKA_SASL_PASSWORD"),
        }
    )

producer = Producer(kafka_config)


def delivery_report(err, msg) -> None:
    """Report Kafka delivery without console-encoding-dependent characters."""
    if err is not None:
        print(f"Kafka chatlog delivery failed: {err}")
    else:
        print(f"Kafka chatlog delivered [topic={msg.topic()} partition={msg.partition()}]")


def send_chat_turn_to_kafka(
    *,
    session_id: str,
    question: str,
    answer: str,
    is_document_rag: bool,
    model: str,
    standalone_query: str | None = None,
    emotion: dict | None = None,
    safety: dict | None = None,
    user_id: str | None = None,
    user_age: int | None = None,
    user_gender: str | None = None,
    learner_type: str | None = None,
    grade: str | int | None = None,
    class_level: str | int | None = None,
    user_group: str | None = None,
    survey_type: str | None = None,
    survey_completed: bool | None = None,
) -> None:
    if os.getenv("CHAT_KAFKA_ENABLED", "true").strip().lower() not in {"1", "true", "yes", "on"}:
        return

    now = datetime.now(UTC)
    anonymous_session_id = anonymize_session_id(session_id)
    audience_group = user_group or audience_group_for_age(user_age)

    event = {
        "event_id": str(uuid4()),
        "event_type": "rag_chat_turn",
        "timestamp": now.isoformat(),
        "anonymous_session_id": anonymous_session_id,
        "user_id_hash": hash_user_id(user_id) if user_id else None,
        "user_age": user_age,
        "user_gender": user_gender,
        "learner_type": learner_type,
        "grade": grade,
        "class_level": class_level if class_level is not None else grade or learner_type,
        "user_group": audience_group,
        "audience_group": audience_group,
        "survey_type": survey_type,
        "survey_completed": survey_completed,
        "question": mask_pii(question),
        "answer": mask_pii(answer),
        "standalone_query": mask_pii(standalone_query) if standalone_query else None,
        "model": model,
        "is_document_rag": is_document_rag,
        "emotion": emotion or {},
        "safety": safety or {},
    }

    producer.produce(
<<<<<<< HEAD
        topic=KAFKA_TOPIC,
        value=json.dumps(event, ensure_ascii=False).encode('utf-8'),
        callback=delivery_report
    )
    # Kích hoạt sự kiện gửi đi ngay lập tức khỏi hàng đợi nội bộ của ứng dụng
    producer.poll(0)
    producer.flush(5)
=======
        topic=CHAT_TOPIC,
        key=(anonymous_session_id or event["event_id"]).encode("utf-8"),
        value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
        callback=delivery_report,
    )
    pending_messages = producer.flush(35)
    if pending_messages:
        raise RuntimeError(f"Kafka delivery timed out with {pending_messages} pending message(s).")
>>>>>>> a737070ebdd229bed647412b6b52a70a9aba65ba
