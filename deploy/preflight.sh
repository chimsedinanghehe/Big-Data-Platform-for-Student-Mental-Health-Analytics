#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

ENV_FILE="${ENV_FILE:-.env.production}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.production.yml}"
SECRET_FILE="${SECRET_FILE:-secrets/gcp-service-account.json}"

command -v docker >/dev/null 2>&1 || {
  echo "Docker is not installed or not available on PATH." >&2
  exit 1
}

[ -f "$ENV_FILE" ] || {
  echo "Missing deploy/.env.production. Copy .env.production.example and fill production values." >&2
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

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" config --quiet

services="$(docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" config --services)"
for service in postgres backend frontend dashboard; do
  echo "$services" | grep -qx "$service" || {
    echo "Production compose is missing required service: $service" >&2
    exit 1
  }
done

for forbidden_service in caddy cloudflared; do
  if echo "$services" | grep -qx "$forbidden_service"; then
    echo "Production compose must use host Nginx, not ${forbidden_service}." >&2
    exit 1
  fi
done

echo "Production preflight passed."
