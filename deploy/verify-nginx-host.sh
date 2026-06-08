#!/usr/bin/env bash
set -euo pipefail

DOMAIN="${1:-${APP_DOMAIN:-mindschool.site}}"
BASE_URL="https://${DOMAIN}"

echo "Checking ${BASE_URL}/"
curl -fsS --max-time 20 "${BASE_URL}/" >/dev/null

echo "Checking ${BASE_URL}/health"
health="$(curl -fsS --max-time 20 "${BASE_URL}/health")"
grep -q '"status":"ok"' <<< "$health"

echo "Checking ${BASE_URL}/ready"
ready="$(curl -fsS --max-time 20 "${BASE_URL}/ready")"
grep -q '"status":"ready"' <<< "$ready"

echo "Checking ${BASE_URL}/dashboard/_stcore/health"
dashboard_health="$(curl -fsS --max-time 20 "${BASE_URL}/dashboard/_stcore/health")"
[[ "${dashboard_health//$'\r'/}" == "ok" ]]

docker compose --env-file deploy/.env.production -f deploy/docker-compose.nginx-host.yml ps
echo "Nginx-host deployment verification passed for ${DOMAIN}."
