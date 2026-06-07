#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
ARCHIVE="${ARCHIVE:-/tmp/mindschool-app.tar.gz}"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run this script as root." >&2
  exit 1
fi

mkdir -p "$APP_ROOT"
tar -xzf "$ARCHIVE" -C "$APP_ROOT"
chmod +x "$APP_ROOT"/deploy/*.sh
mkdir -p "$APP_ROOT/deploy/secrets" "$APP_ROOT/deploy/backups" "$APP_ROOT/deploy/runtime"
chmod 700 "$APP_ROOT/deploy/secrets"

echo "Application source extracted to ${APP_ROOT}."
echo "Next: create ${APP_ROOT}/deploy/.env.production and ${APP_ROOT}/deploy/secrets/gcp-service-account.json."
