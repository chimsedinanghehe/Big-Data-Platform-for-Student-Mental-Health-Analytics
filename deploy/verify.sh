#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

[ -f .env.production ] || {
  echo "Missing deploy/.env.production." >&2
  exit 1
}

domain="$(grep -E '^APP_DOMAIN=' .env.production | tail -n 1 | cut -d= -f2-)"

for path in / /health /ready /dashboard/_stcore/health; do
  curl --fail --silent --show-error --max-time 20 "https://${domain}${path}" >/dev/null
  echo "OK https://${domain}${path}"
done
