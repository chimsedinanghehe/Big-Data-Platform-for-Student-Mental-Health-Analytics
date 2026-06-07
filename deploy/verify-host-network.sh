#!/usr/bin/env bash
set -euo pipefail

DOMAIN="${1:-${APP_DOMAIN:-mindschool.site}}"

for url in \
  "https://${DOMAIN}/" \
  "https://${DOMAIN}/health" \
  "https://${DOMAIN}/ready" \
  "https://${DOMAIN}/dashboard/_stcore/health"
do
  echo "Checking ${url}"
  curl -fsS --max-time 20 "$url" >/dev/null
done

docker compose --env-file deploy/.env.production -f deploy/docker-compose.host-network.yml ps
echo "Host-network deployment verification passed for ${DOMAIN}."
