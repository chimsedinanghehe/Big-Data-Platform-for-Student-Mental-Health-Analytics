#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
ENV_FILE="${APP_ROOT}/deploy/.env.production"
GCP_SECRET="${APP_ROOT}/deploy/secrets/gcp-service-account.json"
WORKFLOW_FILE="${APP_ROOT}/deploy/gcp/workflows/nightly_dashboard_refresh.yaml"
DRY_RUN=false

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

env_value() {
  grep -E "^${1}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true
}

project_id="$(env_value GCP_PROJECT_ID)"
region="$(env_value GCP_REGION)"
bucket="$(env_value GCS_BUCKET_NAME)"
workflow_name="$(env_value GCP_WORKFLOW_NAME)"
workflow_service_account="$(env_value GCP_WORKFLOW_SERVICE_ACCOUNT)"
scheduler_name="$(env_value GCP_SCHEDULER_NAME)"
scheduler_cron="$(env_value GCP_SCHEDULER_CRON)"
scheduler_timezone="$(env_value GCP_SCHEDULER_TIMEZONE)"

region="${region:-asia-southeast1}"
workflow_name="${workflow_name:-nightly-dashboard-refresh}"
workflow_service_account="${workflow_service_account:-dashboard-refresh@${project_id}.iam.gserviceaccount.com}"
scheduler_name="${scheduler_name:-nightly-dashboard-refresh-0230-vn}"
scheduler_cron="${scheduler_cron:-30 2 * * *}"
scheduler_timezone="${scheduler_timezone:-Asia/Ho_Chi_Minh}"

run() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
  if [[ "$DRY_RUN" == false ]]; then
    "$@"
  fi
}

if [[ "$DRY_RUN" == false ]]; then
  command -v gcloud >/dev/null 2>&1 || {
    echo "gcloud is required to deploy Dataproc Workflow automation." >&2
    exit 1
  }
  [[ -f "$GCP_SECRET" ]] || {
    echo "Missing ${GCP_SECRET}." >&2
    exit 1
  }
  credential_type="$(
    python3 - "$GCP_SECRET" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as credential_file:
    print(json.load(credential_file).get("type", ""))
PY
  )"
  case "$credential_type" in
    service_account)
      run gcloud auth activate-service-account --key-file="$GCP_SECRET" --project="$project_id"
      ;;
    authorized_user)
      gcloud auth list --filter=status:ACTIVE --format='value(account)' | grep -q . || {
        echo "authorized_user runtime credential found, but gcloud has no active account." >&2
        exit 1
      }
      ;;
    *)
      echo "Unsupported GCP credential type: ${credential_type}" >&2
      exit 1
      ;;
  esac
  run gcloud services enable \
    dataproc.googleapis.com \
    workflows.googleapis.com \
    workflowexecutions.googleapis.com \
    cloudscheduler.googleapis.com \
    --project="$project_id"
fi

uploads=(
  "MentalSchool_Dashboard/survey_bronze_to_silver_spark.py:scripts/survey_pipeline/survey_bronze_to_silver_spark.py"
  "MentalSchool_Dashboard/survey_silver_to_gold_spark.py:scripts/survey_pipeline/survey_silver_to_gold_spark.py"
  "MentalSchool_Dashboard/chat_bronze_to_silver_spark.py:scripts/chat_pipeline/chat_bronze_to_silver_spark.py"
  "MentalSchool_Dashboard/chat_silver_to_gold_spark.py:scripts/chat_pipeline/chat_silver_to_gold_spark.py"
  "MentalSchool_Dashboard/chat_kafka_to_silver_streaming.py:scripts/chat_pipeline/chat_kafka_to_silver_streaming.py"
)

for mapping in "${uploads[@]}"; do
  source_path="${mapping%%:*}"
  target_path="${mapping#*:}"
  [[ -f "${APP_ROOT}/${source_path}" ]] || {
    echo "Missing pipeline script: ${APP_ROOT}/${source_path}" >&2
    exit 1
  }
  run gcloud storage cp "${APP_ROOT}/${source_path}" "gs://${bucket}/${target_path}"
done

run gcloud workflows deploy "$workflow_name" \
  --project="$project_id" \
  --location="$region" \
  --source="$WORKFLOW_FILE" \
  --service-account="$workflow_service_account"

execution_uri="https://workflowexecutions.googleapis.com/v1/projects/${project_id}/locations/${region}/workflows/${workflow_name}/executions"
message_body="$(
  printf '{"argument":"{\\"project_id\\":\\"%s\\",\\"region\\":\\"%s\\",\\"bucket\\":\\"%s\\"}"}' \
    "$project_id" "$region" "$bucket"
)"

scheduler_action=create
if [[ "$DRY_RUN" == false ]] && gcloud scheduler jobs describe "$scheduler_name" \
  --project="$project_id" --location="$region" >/dev/null 2>&1; then
  scheduler_action=update
fi

run gcloud scheduler jobs "$scheduler_action" http "$scheduler_name" \
  --project="$project_id" \
  --location="$region" \
  --schedule="$scheduler_cron" \
  --time-zone="$scheduler_timezone" \
  --uri="$execution_uri" \
  --http-method=POST \
  --oauth-service-account-email="$workflow_service_account" \
  --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform \
  --headers=Content-Type=application/json \
  --message-body="$message_body"

if [[ "$DRY_RUN" == true ]]; then
  echo "Big Data automation dry-run passed: ${workflow_name}, schedule ${scheduler_cron} ${scheduler_timezone}."
else
  echo "Big Data automation deployed: ${workflow_name}, schedule ${scheduler_cron} ${scheduler_timezone}."
fi
