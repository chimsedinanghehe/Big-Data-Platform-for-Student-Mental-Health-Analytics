param(
    [string]$Bucket = "student-mental-health-lake-nhom1-2026",
    [string]$GcloudPath = "C:\Users\Admin\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")

if (-not (Test-Path $GcloudPath)) {
    $cmd = Get-Command gcloud -ErrorAction SilentlyContinue
    if ($null -eq $cmd) {
        throw "gcloud was not found. Install Google Cloud SDK or pass -GcloudPath."
    }
    $GcloudPath = $cmd.Source
}

$uploads = @(
    @{
        Source = "MentalSchool_Dashboard\chat_bronze_to_silver_spark.py"
        Target = "gs://$Bucket/scripts/chat_pipeline/chat_bronze_to_silver_spark.py"
    },
    @{
        Source = "MentalSchool_Dashboard\chat_kafka_to_silver_streaming.py"
        Target = "gs://$Bucket/scripts/chat_pipeline/chat_kafka_to_silver_streaming.py"
    },
    @{
        Source = "MentalSchool_Dashboard\chat_silver_to_gold_spark.py"
        Target = "gs://$Bucket/scripts/chat_pipeline/chat_silver_to_gold_spark.py"
    },
    @{
        Source = "MentalSchool_Dashboard\survey_bronze_to_silver_spark.py"
        Target = "gs://$Bucket/scripts/survey_pipeline/survey_bronze_to_silver_spark.py"
    },
    @{
        Source = "MentalSchool_Dashboard\survey_silver_to_gold_spark.py"
        Target = "gs://$Bucket/scripts/survey_pipeline/survey_silver_to_gold_spark.py"
    },
    @{
        Source = "scripts\kafka\run_kafka_consumer.py"
        Target = "gs://$Bucket/scripts/kafka/run_kafka_consumer.py"
    },
    @{
        Source = "scripts\kafka\setup_chat_kafka_vm.sh"
        Target = "gs://$Bucket/scripts/kafka/setup_chat_kafka_vm.sh"
    },
    @{
        Source = "scripts\kafka\run_survey_snapshot_consumer.py"
        Target = "gs://$Bucket/scripts/kafka/run_survey_snapshot_consumer.py"
    },
    @{
        Source = "scripts\e2e\submit_test_survey.py"
        Target = "gs://$Bucket/scripts/e2e/submit_test_survey.py"
    },
    @{
        Source = "scripts\e2e\submit_test_surveys_batch.py"
        Target = "gs://$Bucket/scripts/e2e/submit_test_surveys_batch.py"
    },
    @{
        Source = "scripts\scheduler\nightly_dashboard_refresh.ps1"
        Target = "gs://$Bucket/scripts/scheduler/nightly_dashboard_refresh.ps1"
    },
    @{
        Source = "scripts\scheduler\register_gcp_nightly_dashboard_refresh.ps1"
        Target = "gs://$Bucket/scripts/scheduler/register_gcp_nightly_dashboard_refresh.ps1"
    },
    @{
        Source = "scripts\scheduler\survey_no_kafka_dashboard_refresh.ps1"
        Target = "gs://$Bucket/scripts/scheduler/survey_no_kafka_dashboard_refresh.ps1"
    },
    @{
        Source = "deploy\gcp\workflows\nightly_dashboard_refresh.yaml"
        Target = "gs://$Bucket/scripts/scheduler/nightly_dashboard_refresh.workflow.yaml"
    }
)

foreach ($item in $uploads) {
    $sourcePath = Join-Path $ProjectRoot $item.Source
    if (-not (Test-Path $sourcePath)) {
        throw "Missing local file: $sourcePath"
    }
    Write-Host "Uploading $($item.Source) -> $($item.Target)"
    if ($DryRun) {
        Write-Host "[DRY-RUN] $GcloudPath storage cp $sourcePath $($item.Target)"
        continue
    }
    & $GcloudPath storage cp $sourcePath $item.Target
}

Write-Host "Dashboard pipeline scripts uploaded to gs://$Bucket/scripts/"
