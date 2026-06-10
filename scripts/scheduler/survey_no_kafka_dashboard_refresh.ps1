param(
    [string]$ProjectId = "student-mental-health-496205",
    [string]$Region = "asia-southeast1",
    [string]$Bucket = "student-mental-health-lake-nhom1-2026",
    [string]$ApiBaseUrl = "http://127.0.0.1:8000",
    [string]$GcloudPath = "C:\Users\Admin\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
    [int]$Age = 17,
    [string]$Gender = "female",
    [string]$LearnerType = "high_school",
    [switch]$SkipSurveySubmit,
    [switch]$SkipDataproc,
    [switch]$IncludeHeavySurveyTables,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$Python = Join-Path $ProjectRoot "venv\Scripts\python.exe"
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddHHmmss")
$runId = "survey_nokafka_e2e_$stamp"
$logsDir = Join-Path $ProjectRoot "logs"
$surveyResultJson = Join-Path $logsDir "survey_no_kafka_submit_$stamp.json"

if (-not (Test-Path $Python)) {
    throw "Missing project Python: $Python"
}

if (-not (Test-Path $GcloudPath)) {
    $cmd = Get-Command gcloud -ErrorAction SilentlyContinue
    if ($null -eq $cmd) {
        throw "gcloud was not found. Install Google Cloud SDK or pass -GcloudPath."
    }
    $GcloudPath = $cmd.Source
}

$commonSparkProps = @(
    "spark.dynamicAllocation.enabled=false",
    "spark.executor.instances=2",
    "spark.executor.cores=4",
    "spark.executor.memory=8g",
    "spark.driver.cores=4",
    "spark.driver.memory=8g",
    "spark.default.parallelism=8",
    "spark.sql.shuffle.partitions=8",
    "spark.sql.adaptive.enabled=true",
    "spark.sql.adaptive.coalescePartitions.enabled=true",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
    "spark.dataproc.driver.disk.size=250g",
    "spark.dataproc.executor.disk.size=250g"
) -join ","

function Submit-PysparkBatch {
    param(
        [string]$BatchName,
        [string]$MainPythonFileUri,
        [string[]]$JobArgs
    )

    Write-Host "Submitting Dataproc batch: $BatchName" -ForegroundColor Cyan
    if ($DryRun) {
        Write-Host "[DRY-RUN] $GcloudPath dataproc batches submit pyspark $MainPythonFileUri --region=$Region --project=$ProjectId --batch=$BatchName --properties=$commonSparkProps -- $($JobArgs -join ' ')"
        return
    }

    & $GcloudPath dataproc batches submit pyspark $MainPythonFileUri `
        --region=$Region `
        --project=$ProjectId `
        --batch=$BatchName `
        --properties=$commonSparkProps `
        -- `
        @JobArgs
}

Set-Location $ProjectRoot

if (-not $SkipSurveySubmit) {
    Write-Host "Submitting one local API survey response without Kafka..." -ForegroundColor Cyan
    if ($DryRun) {
        Write-Host "[DRY-RUN] $Python scripts\e2e\submit_test_survey.py --api-base-url $ApiBaseUrl --age $Age --gender $Gender --learner-type $LearnerType --output-json $surveyResultJson"
    } else {
        & $Python scripts\e2e\submit_test_survey.py `
            --api-base-url $ApiBaseUrl `
            --age $Age `
            --gender $Gender `
            --learner-type $LearnerType `
            --output-json $surveyResultJson
    }
}

Write-Host "Exporting pending survey responses to GCS Bronze snapshot..." -ForegroundColor Cyan
if ($DryRun) {
    Write-Host "[DRY-RUN] $Python scripts\run_survey_snapshot_worker.py --once --limit 100"
} else {
    $env:SURVEY_KAFKA_ENABLED = "false"
    & $Python scripts\run_survey_snapshot_worker.py --once --limit 100
}

if (-not $SkipDataproc) {
    Submit-PysparkBatch `
        -BatchName "survey-nokafka-b2s-$stamp" `
        -MainPythonFileUri "gs://$Bucket/scripts/survey_pipeline/survey_bronze_to_silver_spark.py" `
        -JobArgs @(
            "--fast-mode",
            "--output-partitions", "4",
            "--spark-parallelism", "8",
            "--shuffle-partitions", "8"
        )

    Submit-PysparkBatch `
        -BatchName "survey-nokafka-s2g-$stamp" `
        -MainPythonFileUri "gs://$Bucket/scripts/survey_pipeline/survey_silver_to_gold_spark.py" `
        -JobArgs @(
            "--tables", "core",
            "--disable-temp-stage",
            "--analytic-construct-mode", "fast",
            "--gold-output-partitions", "4",
            "--analytic-compute-partitions", "0",
            "--temp-output-partitions", "4",
            "--spark-parallelism", "8",
            "--shuffle-partitions", "8",
            "--run-id", $runId
        )

    if ($IncludeHeavySurveyTables) {
        Submit-PysparkBatch `
            -BatchName "survey-nokafka-s2g-heavy-$stamp" `
            -MainPythonFileUri "gs://$Bucket/scripts/survey_pipeline/survey_silver_to_gold_spark.py" `
            -JobArgs @(
                "--tables", "heavy",
                "--disable-temp-stage",
                "--analytic-construct-mode", "fast",
                "--gold-output-partitions", "4",
                "--analytic-compute-partitions", "0",
                "--temp-output-partitions", "4",
                "--spark-parallelism", "8",
                "--shuffle-partitions", "8",
                "--run-id", $runId
            )
    }

    Write-Host "Updating Survey Gold current manifest..." -ForegroundColor Cyan
    & $Python scripts\update_survey_gold_current_manifest.py
}

Write-Host "Survey no-Kafka refresh completed or submitted. run_id=$runId" -ForegroundColor Green
Write-Host "Local submit report: $surveyResultJson"
Write-Host "Bronze snapshot: gs://$Bucket/bronze/app_survey_snapshot/survey_all.parquet"
Write-Host "Gold dashboard root: gs://$Bucket/gold/dashboard_tables/"
