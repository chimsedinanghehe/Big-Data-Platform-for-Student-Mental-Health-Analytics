#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

./preflight.sh
if docker compose --env-file .env.production -f docker-compose.production.yml ps --status running --services | grep -qx postgres; then
  ./backup-postgres.sh
fi
docker compose --env-file .env.production -f docker-compose.production.yml build --pull
docker compose --env-file .env.production -f docker-compose.production.yml up -d --remove-orphans
docker compose --env-file .env.production -f docker-compose.production.yml ps
