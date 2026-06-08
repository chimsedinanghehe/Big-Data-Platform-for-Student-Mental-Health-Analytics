#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
SYSTEMD_DIR="/etc/systemd/system"

[[ "$(id -u)" -eq 0 ]] || {
  echo "Run this script as root." >&2
  exit 1
}

install -m 0644 \
  "${APP_ROOT}/deploy/systemd/mindschool-bigdata-refresh.service" \
  "${SYSTEMD_DIR}/mindschool-bigdata-refresh.service"
install -m 0644 \
  "${APP_ROOT}/deploy/systemd/mindschool-bigdata-refresh.timer" \
  "${SYSTEMD_DIR}/mindschool-bigdata-refresh.timer"

systemctl daemon-reload
systemctl enable --now mindschool-bigdata-refresh.timer
systemctl list-timers mindschool-bigdata-refresh.timer --no-pager
