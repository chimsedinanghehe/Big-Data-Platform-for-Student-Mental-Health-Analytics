#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
COMPOSE_FILE="${APP_ROOT}/deploy/docker-compose.production.yml"
BIGDATA_COMPOSE_FILE="${APP_ROOT}/deploy/docker-compose.bigdata.yml"
ENV_FILE="${APP_ROOT}/deploy/.env.production"
SECRET_FILE="${APP_ROOT}/deploy/secrets/gcp-service-account.json"
HTTP_NGINX_CONF="${APP_ROOT}/deploy/nginx/conf.d/mindschool.site.http-only.conf"
HTTPS_NGINX_CONF="${APP_ROOT}/deploy/nginx/conf.d/mindschool.site.conf"
HOST_NGINX_CONF="/etc/nginx/conf.d/mindschool.site.conf"

cd "$APP_ROOT/deploy"

env_value() {
  grep -E "^${1}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true
}

ENV_FILE="$ENV_FILE" COMPOSE_FILE="$COMPOSE_FILE" SECRET_FILE="$SECRET_FILE" \
  "${APP_ROOT}/deploy/preflight.sh"

compose_args=(--env-file "$ENV_FILE" -f "$COMPOSE_FILE")
bigdata_enabled="$(env_value BIGDATA_ENABLED || true)"
case "${bigdata_enabled,,}" in
  1|true|yes|on)
    APP_ROOT="$APP_ROOT" "${APP_ROOT}/deploy/bigdata-preflight.sh"
    systemctl is-active --quiet mindschool-kafka-tunnel.service || {
      echo "Big Data is enabled but mindschool-kafka-tunnel.service is not active." >&2
      exit 1
    }
    compose_args+=(-f "$BIGDATA_COMPOSE_FILE")
    ;;
esac

docker compose "${compose_args[@]}" config --quiet

if [[ -f /etc/letsencrypt/live/mindschool.site/fullchain.pem ]]; then
  install -m 0644 "$HTTPS_NGINX_CONF" "$HOST_NGINX_CONF"
else
  install -m 0644 "$HTTP_NGINX_CONF" "$HOST_NGINX_CONF"
fi
nginx -t
systemctl reload nginx

if docker compose "${compose_args[@]}" ps --status running --services | grep -qx postgres; then
  COMPOSE_FILE="$COMPOSE_FILE" ENV_FILE="$ENV_FILE" "${APP_ROOT}/deploy/backup-postgres.sh"
fi

docker compose "${compose_args[@]}" up \
  -d --build --remove-orphans --wait --wait-timeout 300
docker compose "${compose_args[@]}" ps

echo "Full Nginx deployment started: PostgreSQL, Backend, Frontend, and Dashboard."
case "${bigdata_enabled,,}" in
  1|true|yes|on) echo "Big Data integration started: Kafka tunnel, GCS credentials, and Survey snapshot worker." ;;
esac
