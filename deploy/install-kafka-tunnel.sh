#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
ENV_FILE="${APP_ROOT}/deploy/.env.production"
KAFKA_SSH_KEY="${APP_ROOT}/deploy/secrets/kafka-vm-ssh-key"
UNIT_FILE="/etc/systemd/system/mindschool-kafka-tunnel.service"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run this script as root." >&2
  exit 1
fi

env_value() {
  grep -E "^${1}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true
}

[[ -f "$ENV_FILE" ]] || {
  echo "Missing ${ENV_FILE}." >&2
  exit 1
}

[[ -f "$KAFKA_SSH_KEY" ]] || {
  echo "Missing ${KAFKA_SSH_KEY}." >&2
  exit 1
}

chown root:root "$KAFKA_SSH_KEY"
chmod 600 "$KAFKA_SSH_KEY"

kafka_host="$(env_value KAFKA_SSH_HOST)"
kafka_user="$(env_value KAFKA_SSH_USER)"
kafka_ssh_port="$(env_value KAFKA_SSH_PORT)"
kafka_local_port="$(env_value KAFKA_LOCAL_PORT)"
kafka_remote_host="$(env_value KAFKA_REMOTE_HOST)"
kafka_remote_port="$(env_value KAFKA_REMOTE_PORT)"

for value_name in kafka_host kafka_user kafka_ssh_port kafka_local_port kafka_remote_host kafka_remote_port; do
  [[ -n "${!value_name}" ]] || {
    echo "Missing Kafka tunnel value: ${value_name}" >&2
    exit 1
  }
done

timeout 10 ssh \
  -p "$kafka_ssh_port" \
  -i "$KAFKA_SSH_KEY" \
  -o BatchMode=yes \
  -o ConnectTimeout=8 \
  -o StrictHostKeyChecking=accept-new \
  "${kafka_user}@${kafka_host}" true || {
    echo "Kafka VM SSH authentication failed." >&2
    exit 1
  }

cat > "$UNIT_FILE" <<EOF
[Unit]
Description=MindSchool Kafka SSH tunnel
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/ssh -NT -p ${kafka_ssh_port} -i ${KAFKA_SSH_KEY} -o BatchMode=yes -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=accept-new -L 127.0.0.1:${kafka_local_port}:${kafka_remote_host}:${kafka_remote_port} ${kafka_user}@${kafka_host}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now mindschool-kafka-tunnel.service

for _attempt in $(seq 1 20); do
  if timeout 3 bash -c "</dev/tcp/127.0.0.1/${kafka_local_port}" 2>/dev/null; then
    echo "Kafka tunnel is listening on 127.0.0.1:${kafka_local_port}."
    exit 0
  fi
  sleep 1
done

systemctl status mindschool-kafka-tunnel.service --no-pager >&2 || true
echo "Kafka tunnel failed to become ready." >&2
exit 1
