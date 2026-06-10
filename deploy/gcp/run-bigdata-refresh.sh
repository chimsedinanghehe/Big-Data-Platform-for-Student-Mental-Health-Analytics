#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
ENV_FILE="${APP_ROOT}/deploy/.env.production"
DRY_RUN=false
PROCESS_DATE="${PROCESS_DATE:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --process-date)
      PROCESS_DATE="${2:?Missing value for --process-date}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

env_value() {
  grep -E "^${1}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true
}

[[ -f "$ENV_FILE" ]] || {
  echo "Missing ${ENV_FILE}." >&2
  exit 1
}

GCLOUD_BIN="${GCLOUD_BIN:-/snap/bin/gcloud}"
[[ -x "$GCLOUD_BIN" ]] || GCLOUD_BIN="$(command -v gcloud || true)"
[[ -n "$GCLOUD_BIN" && -x "$GCLOUD_BIN" ]] || {
  echo "gcloud is required for Dataproc Serverless refresh." >&2
  exit 1
}

project_id="$(env_value GCP_PROJECT_ID)"
region="$(env_value GCP_REGION)"
bucket="$(env_value GCS_BUCKET_NAME)"
service_account="$(env_value GCP_WORKFLOW_SERVICE_ACCOUNT)"
region="${region:-asia-southeast1}"
PROCESS_DATE="${PROCESS_DATE:-$(TZ=Asia/Ho_Chi_Minh date +%F)}"

for value_name in project_id region bucket service_account PROCESS_DATE; do
  [[ -n "${!value_name}" ]] || {
    echo "Missing refresh value: ${value_name}" >&2
    exit 1
  }
done

stamp="$(TZ=Asia/Ho_Chi_Minh date +%Y%m%d-%H%M%S)"
batch_prefix="ms-${stamp}"
properties="spark.dynamicAllocation.enabled=false,spark.executor.instances=2,spark.executor.cores=4,spark.executor.memory=8g,spark.driver.cores=4,spark.driver.memory=8g,spark.default.parallelism=8,spark.sql.shuffle.partitions=8,spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true,spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2,spark.dataproc.driver.disk.size=250g,spark.dataproc.executor.disk.size=250g"

run() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
  if [[ "$DRY_RUN" == false ]]; then
    "$@"
  fi
}

submit() {
  local batch_id="$1"
  local script_uri="$2"
  shift 2
  run "$GCLOUD_BIN" dataproc batches submit pyspark "$script_uri" \
    --project="$project_id" \
    --region="$region" \
    --batch="$batch_id" \
    --service-account="$service_account" \
    --properties="$properties" \
    -- "$@"
}

submit "${batch_prefix}-survey-b2s" \
  "gs://${bucket}/scripts/survey_pipeline/survey_bronze_to_silver_spark.py" \
  --fast-mode \
  --output-partitions 4 \
  --spark-parallelism 8 \
  --shuffle-partitions 8

submit "${batch_prefix}-survey-s2g" \
  "gs://${bucket}/scripts/survey_pipeline/survey_silver_to_gold_spark.py" \
  --tables core \
  --disable-temp-stage \
  --analytic-construct-mode fast \
  --gold-output-partitions 4 \
  --analytic-compute-partitions 0 \
  --temp-output-partitions 4 \
  --spark-parallelism 8 \
  --shuffle-partitions 8 \
  --run-id "$batch_prefix"

submit "${batch_prefix}-chat-b2s" \
  "gs://${bucket}/scripts/chat_pipeline/chat_bronze_to_silver_spark.py" \
  --process-date "$PROCESS_DATE" \
  --fast-mode \
  --output-partitions 4 \
  --spark-parallelism 8 \
  --shuffle-partitions 8

submit "${batch_prefix}-chat-s2g" \
  "gs://${bucket}/scripts/chat_pipeline/chat_silver_to_gold_spark.py" \
  --tables all \
  --fast-mode \
  --gold-output-partitions 4 \
  --spark-parallelism 8 \
  --shuffle-partitions 8

run docker compose \
  --env-file "$ENV_FILE" \
  -f "${APP_ROOT}/deploy/docker-compose.production.yml" \
  -f "${APP_ROOT}/deploy/docker-compose.bigdata.yml" \
  exec -T dashboard \
  python /app/warm_dashboard_cache.py

if [[ "$DRY_RUN" == true ]]; then
  echo "Big Data refresh dry-run passed for ${PROCESS_DATE}."
else
  echo "Big Data refresh completed for ${PROCESS_DATE}."
fi
