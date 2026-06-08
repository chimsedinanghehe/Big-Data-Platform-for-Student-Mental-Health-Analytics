#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
APP_ROOT="$APP_ROOT" "${APP_ROOT}/deploy/deploy.sh"
