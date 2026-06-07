from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(PROJECT_ROOT / "backend" / ".env", override=True)

from backend.surveys.kafka_publisher import SURVEY_TOPIC
from backend.surveys.snapshot_worker import export_survey_response_ids


def main() -> None:
    consumer = Consumer(kafka_config())
    consumer.subscribe([SURVEY_TOPIC])
    print(f"Survey snapshot Kafka consumer listening on topic {SURVEY_TOPIC}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())

            event = json.loads(msg.value().decode("utf-8"))
            survey_response_id = event.get("survey_response_id")
            result = export_survey_response_ids([survey_response_id])
            print(json.dumps({"event_id": event.get("event_id"), **result}, sort_keys=True, default=str))
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()


def kafka_config() -> dict[str, object]:
    config: dict[str, object] = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": os.getenv("SURVEY_KAFKA_GROUP_ID", "survey-snapshot-writers"),
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


if __name__ == "__main__":
    main()
