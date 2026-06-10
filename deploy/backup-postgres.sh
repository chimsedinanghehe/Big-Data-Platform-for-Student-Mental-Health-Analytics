#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p backups

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
output="backups/postgres-${stamp}.sql.gz"

docker compose --env-file .env.production -f docker-compose.production.yml exec -T postgres \
  sh -c 'pg_dump -h 127.0.0.1 -p 15432 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' | gzip >"$output"

echo "Created $output"
