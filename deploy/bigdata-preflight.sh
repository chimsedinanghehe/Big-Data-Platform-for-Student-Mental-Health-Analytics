#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
ENV_FILE="${APP_ROOT}/deploy/.env.production"
GCP_SECRET="${APP_ROOT}/deploy/secrets/gcp-service-account.json"
KAFKA_SSH_KEY="${APP_ROOT}/deploy/secrets/kafka-vm-ssh-key"
GCP_SECRET_UID="${GCP_SECRET_UID:-10001}"

env_value() {
  grep -E "^${1}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true
}

[[ -f "$ENV_FILE" ]] || {
  echo "Missing ${ENV_FILE}." >&2
  exit 1
}

[[ -f "$GCP_SECRET" ]] || {
  echo "Missing ${GCP_SECRET}." >&2
  exit 1
}

[[ "$(id -u)" -eq 0 ]] || {
  echo "Run Big Data preflight as root so production secret ownership can be enforced." >&2
  exit 1
}

[[ "$GCP_SECRET_UID" =~ ^[0-9]+$ ]] || {
  echo "GCP_SECRET_UID must be numeric." >&2
  exit 1
}

chown "${GCP_SECRET_UID}:${GCP_SECRET_UID}" "$GCP_SECRET"
chmod 400 "$GCP_SECRET"

python3 - "$GCP_SECRET" <<'PY'
import base64
import json
import sys

with open(sys.argv[1], encoding="utf-8") as credential_file:
    credential = json.load(credential_file)

credential_type = credential.get("type")
if credential_type == "service_account":
    required = ("client_email", "private_key", "project_id")
    if any(not credential.get(key) for key in required):
        raise SystemExit("GCP service-account credential is missing required values.")

    private_key = credential["private_key"].strip()
    lines = private_key.splitlines()
    if (
        len(lines) < 3
        or lines[0] != "-----BEGIN PRIVATE KEY-----"
        or lines[-1] != "-----END PRIVATE KEY-----"
    ):
        raise SystemExit("GCP credential private_key must be a PEM private key.")

    body = "".join(lines[1:-1])
    if len(body) < 1000:
        raise SystemExit("GCP credential private_key appears truncated.")
    try:
        base64.b64decode(body, validate=True)
    except Exception as exc:
        raise SystemExit("GCP credential private_key has invalid PEM base64.") from exc
elif credential_type == "authorized_user":
    required = ("client_id", "client_secret", "refresh_token", "quota_project_id")
    if any(not credential.get(key) for key in required):
        raise SystemExit("GCP authorized-user credential is missing required values.")
else:
    raise SystemExit("GCP credential must be service_account or authorized_user JSON.")
PY

[[ -f "$KAFKA_SSH_KEY" ]] || {
  echo "Missing ${KAFKA_SSH_KEY}." >&2
  exit 1
}

chown root:root "$KAFKA_SSH_KEY"
chmod 600 "$KAFKA_SSH_KEY"

for key in GCP_PROJECT_ID GCP_REGION GCS_BUCKET_NAME KAFKA_SSH_HOST KAFKA_SSH_USER KAFKA_SSH_PORT KAFKA_LOCAL_PORT KAFKA_REMOTE_HOST KAFKA_REMOTE_PORT; do
  value="$(env_value "$key")"
  [[ -n "$value" ]] || {
    echo "Missing Big Data production value: ${key}" >&2
    exit 1
  }
done

kafka_host="$(env_value KAFKA_SSH_HOST)"
kafka_user="$(env_value KAFKA_SSH_USER)"
kafka_port="$(env_value KAFKA_SSH_PORT)"

[[ "$kafka_host" =~ ^[A-Za-z0-9._:-]+$ ]] || {
  echo "Invalid KAFKA_SSH_HOST." >&2
  exit 1
}
[[ "$kafka_user" =~ ^[A-Za-z0-9._-]+$ ]] || {
  echo "Invalid KAFKA_SSH_USER." >&2
  exit 1
}
[[ "$kafka_port" =~ ^[0-9]+$ ]] || {
  echo "Invalid KAFKA_SSH_PORT." >&2
  exit 1
}

timeout 10 ssh \
  -p "$kafka_port" \
  -i "$KAFKA_SSH_KEY" \
  -o BatchMode=yes \
  -o ConnectTimeout=8 \
  -o StrictHostKeyChecking=accept-new \
  "${kafka_user}@${kafka_host}" true || {
    echo "Kafka VM SSH authentication failed." >&2
    exit 1
  }

echo "Big Data preflight passed."
