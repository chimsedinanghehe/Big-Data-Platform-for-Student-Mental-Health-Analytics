#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

ENV_FILE=".env.production"
COMPOSE_FILE="docker-compose.production.yml"
SECRET_FILE="secrets/gcp-service-account.json"

command -v docker >/dev/null 2>&1 || {
  echo "Docker is not installed or not available on PATH." >&2
  exit 1
}

[ -f "$ENV_FILE" ] || {
  echo "Missing deploy/.env.production. Copy .env.production.example and fill production values." >&2
  exit 1
}

[ -f "$SECRET_FILE" ] || {
  echo "Missing deploy/secrets/gcp-service-account.json." >&2
  exit 1
}

required_keys="
APP_DOMAIN
ACME_EMAIL
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
OPENAI_API_KEY
QDRANT_URL
GCP_PROJECT_ID
GCS_BUCKET_NAME
KAFKA_BOOTSTRAP_SERVERS
"

for key in $required_keys; do
  value="$(grep -E "^${key}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true)"
  case "$value" in
    ""|*replace*|*example.com*)
      echo "Missing or placeholder production value: $key" >&2
      exit 1
      ;;
  esac
done

kafka="$(grep -E '^KAFKA_BOOTSTRAP_SERVERS=' "$ENV_FILE" | tail -n 1 | cut -d= -f2-)"
case "$kafka" in
  127.0.0.1*|localhost*)
    echo "KAFKA_BOOTSTRAP_SERVERS must be reachable from the deployment host/container." >&2
    exit 1
    ;;
esac

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" config --quiet
echo "Production preflight passed."
