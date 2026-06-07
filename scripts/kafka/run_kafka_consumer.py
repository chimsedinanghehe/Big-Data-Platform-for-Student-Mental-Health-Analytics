from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(PROJECT_ROOT / "backend" / ".env", override=True)

CHAT_TOPIC = os.getenv("CHAT_KAFKA_TOPIC", "student-chat-logs")
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME") or os.getenv("GCS_BUCKET", "student-mental-health-lake-nhom1-2026")
GCS_CHATLOG_PREFIX = os.getenv("GCS_CHATLOG_PREFIX", "bronze/chat_logs").strip("/")
MAX_BATCH_SIZE = int(os.getenv("CHAT_KAFKA_MAX_BATCH_SIZE", "50"))
MAX_WAIT_SECONDS = int(os.getenv("CHAT_KAFKA_MAX_WAIT_SECONDS", "15"))


def kafka_config() -> dict[str, object]:
    config: dict[str, object] = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": os.getenv("CHAT_KAFKA_GROUP_ID", "gcs-log-creators"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
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


def event_date(event: dict) -> str:
    timestamp = event.get("timestamp")
    if isinstance(timestamp, str) and timestamp:
        try:
            return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            pass
    return datetime.now(UTC).date().isoformat()


def upload_events_to_gcs(events: list[dict]) -> list[str]:
    if not events:
        return []

    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    written_uris: list[str] = []
    events_by_date: dict[str, list[dict]] = defaultdict(list)
    for event in events:
        events_by_date[event_date(event)].append(event)

    for date_value, date_events in events_by_date.items():
        payload = "".join(
            json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"
            for event in date_events
        )
        object_name = (
            f"{GCS_CHATLOG_PREFIX}/date={date_value}/"
            f"kafka_batch_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}_{uuid4().hex}.jsonl"
        )
        blob = bucket.blob(object_name)
        blob.upload_from_string(payload, content_type="application/jsonl")
        written_uris.append(f"gs://{GCS_BUCKET}/{object_name}")

    return written_uris


def main() -> None:
    consumer = Consumer(kafka_config())
    consumer.subscribe([CHAT_TOPIC])
    print(f"Kafka chat consumer listening on topic {CHAT_TOPIC}")
    print(f"Writing chat Bronze JSONL to gs://{GCS_BUCKET}/{GCS_CHATLOG_PREFIX}/")

    buffer_events: list[dict] = []
    buffer_messages = []
    last_flush_time = time.time()

    def flush_buffer() -> None:
        nonlocal buffer_events, buffer_messages, last_flush_time
        if not buffer_events:
            return
        written_uris = upload_events_to_gcs(buffer_events)
        for message in buffer_messages:
            consumer.commit(message=message, asynchronous=False)
        print(
            json.dumps(
                {
                    "event": "chat_kafka_batch_uploaded",
                    "events": len(buffer_events),
                    "objects": written_uris,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        buffer_events = []
        buffer_messages = []
        last_flush_time = time.time()

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                if buffer_events and (time.time() - last_flush_time >= MAX_WAIT_SECONDS):
                    flush_buffer()
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())

            event = json.loads(message.value().decode("utf-8"))
            print(f"Received chat log from Kafka | event_id={event.get('event_id')}")
            buffer_events.append(event)
            buffer_messages.append(message)

            if len(buffer_events) >= MAX_BATCH_SIZE:
                flush_buffer()
    except KeyboardInterrupt:
        print("Stopping Kafka chat consumer...")
    finally:
        flush_buffer()
        consumer.close()


if __name__ == "__main__":
    main()
