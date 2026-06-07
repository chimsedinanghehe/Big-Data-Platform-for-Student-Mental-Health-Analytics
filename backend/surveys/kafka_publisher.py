from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from uuid import uuid4


SURVEY_TOPIC = os.getenv("SURVEY_KAFKA_TOPIC", "student-survey-events")


def publish_survey_completed_event(*, survey_response_id: str, user_id: str, survey_type: str) -> None:
    """Publish a lightweight survey event when Kafka survey events are enabled.

    The snapshot writer still reloads the row from PostgreSQL, so Kafka carries
    only identifiers and acts as a trigger, not the source of truth.
    """
    if os.getenv("SURVEY_KAFKA_ENABLED", "false").strip().lower() not in {"1", "true", "yes"}:
        return

    from confluent_kafka import Producer

    producer = Producer(_kafka_config())
    event = {
        "event_id": str(uuid4()),
        "event_type": "survey_completed",
        "timestamp": datetime.now(UTC).isoformat(),
        "survey_response_id": survey_response_id,
        "user_id": user_id,
        "survey_type": survey_type,
    }
    producer.produce(SURVEY_TOPIC, key=user_id.encode("utf-8"), value=json.dumps(event).encode("utf-8"))
    pending_messages = producer.flush(35)
    if pending_messages:
        raise RuntimeError(f"Kafka survey delivery timed out with {pending_messages} pending message(s).")


def _kafka_config() -> dict[str, object]:
    config: dict[str, object] = {
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
        config.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": os.getenv("KAFKA_SASL_USERNAME", ""),
                "sasl.password": os.getenv("KAFKA_SASL_PASSWORD", ""),
            }
        )
    return config
