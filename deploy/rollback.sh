#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

docker compose --env-file .env.production -f docker-compose.production.yml down
echo "Application containers stopped. Persistent PostgreSQL data was preserved."
