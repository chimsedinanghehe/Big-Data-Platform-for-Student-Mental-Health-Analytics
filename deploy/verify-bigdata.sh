#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
ENV_FILE="${APP_ROOT}/deploy/.env.production"
BASE_COMPOSE="${APP_ROOT}/deploy/docker-compose.production.yml"
BIGDATA_COMPOSE="${APP_ROOT}/deploy/docker-compose.bigdata.yml"

env_value() {
  grep -E "^${1}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true
}

APP_ROOT="$APP_ROOT" "${APP_ROOT}/deploy/verify.sh"
systemctl is-active --quiet mindschool-kafka-tunnel.service

kafka_local_port="$(env_value KAFKA_LOCAL_PORT)"
timeout 5 bash -c "</dev/tcp/127.0.0.1/${kafka_local_port}"

docker compose --env-file "$ENV_FILE" -f "$BASE_COMPOSE" -f "$BIGDATA_COMPOSE" ps
worker_status="$(
  docker compose --env-file "$ENV_FILE" -f "$BASE_COMPOSE" -f "$BIGDATA_COMPOSE" \
    ps --format json survey-snapshot-worker
)"
grep -q '"Health":"healthy"' <<< "$worker_status"

docker compose --env-file "$ENV_FILE" -f "$BASE_COMPOSE" -f "$BIGDATA_COMPOSE" \
  exec -T backend python /app/scripts/deployment/production_health_check.py

echo "Big Data production verification passed."
