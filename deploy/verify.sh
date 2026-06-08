#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
domain="$(grep -E '^APP_DOMAIN=' "${APP_ROOT}/deploy/.env.production" | tail -n 1 | cut -d= -f2-)"

"${APP_ROOT}/deploy/verify-host-network.sh" "$domain"
