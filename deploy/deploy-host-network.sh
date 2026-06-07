#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
COMPOSE_FILE="${APP_ROOT}/deploy/docker-compose.host-network.yml"
ENV_FILE="${APP_ROOT}/deploy/.env.production"

cd "$APP_ROOT/deploy"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing ${ENV_FILE}. Create it from .env.production.example and put secrets on the host only." >&2
  exit 1
fi

if [[ ! -f "${APP_ROOT}/deploy/secrets/gcp-service-account.json" ]]; then
  echo "Missing deploy/secrets/gcp-service-account.json. Production GCS reads/writes require this service account." >&2
  exit 1
fi

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" config --quiet
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d --build
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" ps

nginx -t
systemctl reload nginx
