import json
import os
from datetime import datetime, UTC
from pathlib import Path
from uuid import uuid4
from confluent_kafka import Producer
from dotenv import load_dotenv
from backend.chat_logs.gcs_writer import anonymize_session_id, mask_pii

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(BACKEND_ROOT / ".env", override=True)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "student-chat-logs")

# Cấu hình kết nối linh hoạt giữa Local và Confluent Cloud
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
}

# Nếu phát hiện có Username/Password trong file .env, tự động bật cấu hình bảo mật lên Cloud
if os.getenv('KAFKA_SASL_USERNAME'):
    kafka_config.update({
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
        'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    })

producer = Producer(kafka_config)

def delivery_report(err, msg):
    """ Hàm callback hiển thị trạng thái khi tin nhắn gửi tới Kafka thành công/thất bại """
    if err is not None:
        print(f"❌ Gửi chatlog vào Kafka thất bại: {err}")
    else:
        print(f"✅ Đã ghi log vào Kafka thành công! [Topic: {msg.topic()} | Partition: {msg.partition()}]")

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
):
    now = datetime.now(UTC)
    anonymous_session_id = anonymize_session_id(session_id)
    
    # Tạo cấu trúc Event giống hệt định dạng cũ của nhóm bạn
    event = {
        "event_id": str(uuid4()),
        "event_type": "rag_chat_turn",
        "timestamp": now.isoformat(),
        "anonymous_session_id": anonymous_session_id,
        "question": mask_pii(question),     # Giữ nguyên hàm lọc thông tin PII nhạy cảm
        "answer": mask_pii(answer),         # Giữ nguyên hàm lọc thông tin PII nhạy cảm
        "standalone_query": mask_pii(standalone_query) if standalone_query else None,
        "model": model,
        "is_document_rag": is_document_rag,
        "emotion": emotion or {},
        "safety": safety or {},
    }
    
    # Đẩy dữ liệu vào Kafka dưới dạng chuỗi byte JSON
    producer.produce(
        topic=KAFKA_TOPIC,
        value=json.dumps(event, ensure_ascii=False).encode('utf-8'),
        callback=delivery_report
    )
    # Kích hoạt sự kiện gửi đi ngay lập tức khỏi hàng đợi nội bộ của ứng dụng
    producer.poll(0)
    producer.flush(5)
