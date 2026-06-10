#!/usr/bin/env bash
set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-3.9.1}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_ARCHIVE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"
KAFKA_HOME="/opt/kafka"
KAFKA_DATA_DIR="/var/lib/kafka/kraft-combined-logs"
CONSUMER_HOME="/opt/student-chat-consumer"
CHAT_TOPIC="${CHAT_KAFKA_TOPIC:-student-chat-logs}"
CHAT_PARTITIONS="${CHAT_KAFKA_PARTITIONS:-4}"
KAFKA_ADVERTISED_HOST="${KAFKA_ADVERTISED_HOST:-localhost}"

if ! id kafka >/dev/null 2>&1; then
  sudo useradd --system --home /var/lib/kafka --shell /usr/sbin/nologin kafka
fi

if [[ ! -x "${KAFKA_HOME}/bin/kafka-server-start.sh" ]]; then
  work_dir="$(mktemp -d)"
  trap 'rm -rf "${work_dir}"' EXIT
  curl --fail --location --retry 3 --output "${work_dir}/${KAFKA_ARCHIVE}" "${KAFKA_URL}"
  tar -xzf "${work_dir}/${KAFKA_ARCHIVE}" -C "${work_dir}"
  sudo rm -rf "${KAFKA_HOME}"
  sudo mv "${work_dir}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "${KAFKA_HOME}"
fi

sudo mkdir -p "${KAFKA_DATA_DIR}"
sudo chown -R kafka:kafka "${KAFKA_HOME}" /var/lib/kafka

sudo tee "${KAFKA_HOME}/config/kraft/server.properties" >/dev/null <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://${KAFKA_ADVERTISED_HOST}:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=/var/lib/kafka/kraft-combined-logs
num.network.threads=3
num.io.threads=8
num.partitions=4
default.replication.factor=1
min.insync.replicas=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=268435456
log.cleanup.policy=delete
auto.create.topics.enable=false
EOF
sudo chown kafka:kafka "${KAFKA_HOME}/config/kraft/server.properties"

if [[ ! -f "${KAFKA_DATA_DIR}/meta.properties" ]]; then
  cluster_id="$(sudo -u kafka "${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)"
  sudo -u kafka "${KAFKA_HOME}/bin/kafka-storage.sh" format \
    --cluster-id "${cluster_id}" \
    --config "${KAFKA_HOME}/config/kraft/server.properties"
fi

sudo tee /etc/systemd/system/kafka.service >/dev/null <<EOF
[Unit]
Description=Apache Kafka single-node KRaft broker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="KAFKA_HEAP_OPTS=-Xms512m -Xmx1g"
ExecStart=${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/kraft/server.properties
ExecStop=${KAFKA_HOME}/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable kafka.service
sudo systemctl restart kafka.service

for _ in $(seq 1 30); do
  if "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
"${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null

"${KAFKA_HOME}/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "${CHAT_TOPIC}" \
  --partitions "${CHAT_PARTITIONS}" \
  --replication-factor 1

if [[ ! -f /tmp/run_kafka_consumer.py ]]; then
  echo "Missing /tmp/run_kafka_consumer.py. Copy the repository consumer script before running setup." >&2
  exit 1
fi

if ! python3 -c "import ensurepip" >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install -y python3-venv
fi

sudo mkdir -p "${CONSUMER_HOME}"
sudo cp /tmp/run_kafka_consumer.py "${CONSUMER_HOME}/run_kafka_consumer.py"
sudo rm -rf "${CONSUMER_HOME}/venv"
sudo python3 -m venv "${CONSUMER_HOME}/venv"
sudo "${CONSUMER_HOME}/venv/bin/pip" install --disable-pip-version-check \
  confluent-kafka google-cloud-storage python-dotenv
sudo chown -R root:kafka "${CONSUMER_HOME}"
sudo chmod -R g+rX "${CONSUMER_HOME}"

sudo tee /etc/systemd/system/student-chat-kafka-consumer.service >/dev/null <<EOF
[Unit]
Description=Student chat Kafka to GCS Bronze consumer
After=kafka.service network-online.target
Requires=kafka.service

[Service]
Type=simple
User=kafka
Group=kafka
Environment=KAFKA_BOOTSTRAP_SERVERS=localhost:9092
Environment=CHAT_KAFKA_TOPIC=${CHAT_TOPIC}
Environment=CHAT_KAFKA_GROUP_ID=gcs-log-creators-v2
Environment=CHAT_KAFKA_MAX_BATCH_SIZE=50
Environment=CHAT_KAFKA_MAX_WAIT_SECONDS=15
Environment=GCS_BUCKET_NAME=student-mental-health-lake-nhom1-2026
Environment=GCS_CHATLOG_PREFIX=bronze/chat_logs
Environment=PYTHONUNBUFFERED=1
ExecStart=${CONSUMER_HOME}/venv/bin/python ${CONSUMER_HOME}/run_kafka_consumer.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable student-chat-kafka-consumer.service
sudo systemctl restart student-chat-kafka-consumer.service

echo "Kafka and chat consumer are running."
sudo systemctl --no-pager --full status kafka.service student-chat-kafka-consumer.service | head -80
