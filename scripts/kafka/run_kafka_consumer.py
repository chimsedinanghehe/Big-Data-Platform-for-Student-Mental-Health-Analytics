import json
import os
import time
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group.id': 'gcs-log-creators',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

# Tự động kích hoạt cơ chế bảo mật nếu chạy trên Confluent Cloud
if os.getenv('KAFKA_SASL_USERNAME'):
    config.update({
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
        'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    })

consumer = Consumer(config)
consumer.subscribe(['student-chat-logs'])

print("🚀 Kafka Consumer đang chạy ngầm và lắng nghe tin nhắn tại topic 'student-chat-logs'...")

buffer_events = []
MAX_BATCH_SIZE = 5         # Gom đủ 5 tin nhắn sẽ xử lý một lần (bạn có thể tăng lên 50-100 khi chạy thật)
MAX_WAIT_SECONDS = 15      # Hoặc quá 15 giây mà không có tin mới cũng sẽ xử lý bộ đệm
last_flush_time = time.time()

try:
    while True:
        # Lắng nghe tin nhắn từ Kafka với timeout 1.0 giây
        msg = consumer.poll(1.0)
        
        # Trường hợp không có tin nhắn mới trong hàng đợi
        if msg is None:
            # Kiểm tra xem bộ đệm có dữ liệu cũ và đã quá thời gian chờ chưa
            if buffer_events and (time.time() - last_flush_time >= MAX_WAIT_SECONDS):
                print(f"⌛ [Timeout] Gom tụ {len(buffer_events)} logs. Tiến hành xử lý lưu trữ...")
                # Ở đây bạn có thể gọi hàm _upload_jsonl hoặc xử lý ghi file tùy ý
                buffer_events.clear()
                last_flush_time = time.time()
            continue

        # Trường hợp gặp lỗi hệ thống từ Kafka Broker
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"❌ Lỗi Kafka Consumer: {msg.error()}")
                break

        # Nhận tin nhắn thành công, giải mã từ bytes sang chuỗi JSON
        event_data = json.loads(msg.value().decode('utf-8'))
        print(f"📥 Đã nhặt 1 log từ Kafka | Event ID: {event_data.get('event_id')}")
        buffer_events.append(event_data)
        
        # Nếu bộ đệm (buffer) đạt giới hạn số lượng tin nhắn tối đa
        if len(buffer_events) >= MAX_BATCH_SIZE:
            print(f"📦 [Buffer Full] Gom đủ {len(buffer_events)} logs. Tiến hành xử lý lưu trữ...")
            # Xử lý lưu trữ hàng loạt (Batch Ingestion) tại đây
            buffer_events.clear()
            last_flush_time = time.time()

except KeyboardInterrupt:
    print("\n🛑 Đang dừng Kafka Consumer...")
finally:
    consumer.close()