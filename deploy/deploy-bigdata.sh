#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
ENV_FILE="${APP_ROOT}/deploy/.env.production"

set_bigdata_enabled() {
  if grep -q '^BIGDATA_ENABLED=' "$ENV_FILE"; then
    sed -i "s/^BIGDATA_ENABLED=.*/BIGDATA_ENABLED=${1}/" "$ENV_FILE"
  else
    printf '\nBIGDATA_ENABLED=%s\n' "$1" >> "$ENV_FILE"
  fi
}

recover_core_deployment() {
  exit_code=$?
  trap - ERR
  echo "Big Data activation failed; restoring the healthy core deployment." >&2
  set_bigdata_enabled false
  APP_ROOT="$APP_ROOT" "${APP_ROOT}/deploy/deploy.sh" || true
  exit "$exit_code"
}

APP_ROOT="$APP_ROOT" "${APP_ROOT}/deploy/bigdata-preflight.sh"
APP_ROOT="$APP_ROOT" "${APP_ROOT}/deploy/install-kafka-tunnel.sh"

trap recover_core_deployment ERR
set_bigdata_enabled true
APP_ROOT="$APP_ROOT" "${APP_ROOT}/deploy/deploy.sh"
APP_ROOT="$APP_ROOT" "${APP_ROOT}/deploy/verify-bigdata.sh"
trap - ERR
