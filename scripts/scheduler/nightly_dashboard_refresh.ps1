param(
    [string]$ProjectId = "student-mental-health-496205",
    [string]$Region = "asia-southeast1",
    [string]$Bucket = "student-mental-health-lake-nhom1-2026",
    [string]$ProcessDate = (Get-Date).ToString("yyyy-MM-dd"),
    [string]$GcloudPath = "C:\Users\Admin\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
    [switch]$IncludeHeavySurveyTables,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $GcloudPath)) {
    $cmd = Get-Command gcloud -ErrorAction SilentlyContinue
    if ($null -eq $cmd) {
        throw "gcloud was not found. Install Google Cloud SDK or pass -GcloudPath."
    }
    $GcloudPath = $cmd.Source
}

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddHHmmss")
$safeDate = $ProcessDate.Replace("-", "")
$runId = "nightly-$safeDate-$stamp"
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

    Write-Host "Submitting Dataproc batch: $BatchName"
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

# Survey refresh: Kafka consumer/worker should maintain bronze/app_survey_snapshot.
# This batch only refreshes Spark layers for dashboard consumption.
Submit-PysparkBatch `
    -BatchName "survey-b2s-$safeDate-$stamp" `
    -MainPythonFileUri "gs://$Bucket/scripts/survey_pipeline/survey_bronze_to_silver_spark.py" `
    -JobArgs @(
        "--fast-mode",
        "--output-partitions", "4",
        "--spark-parallelism", "8",
        "--shuffle-partitions", "8"
    )

Submit-PysparkBatch `
    -BatchName "survey-s2g-core-$safeDate-$stamp" `
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
        -BatchName "survey-s2g-heavy-$safeDate-$stamp" `
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

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$Python = Join-Path $ProjectRoot "venv\Scripts\python.exe"
Write-Host "Updating Survey Gold current manifest..."
& $Python (Join-Path $ProjectRoot "scripts\update_survey_gold_current_manifest.py")

# Chat refresh: if Kafka streaming is running, Silver is already being appended.
# The batch below is a safe daily backfill from Bronze for the target date.
Submit-PysparkBatch `
    -BatchName "chat-b2s-$safeDate-$stamp" `
    -MainPythonFileUri "gs://$Bucket/scripts/chat_pipeline/chat_bronze_to_silver_spark.py" `
    -JobArgs @(
        "--process-date", $ProcessDate,
        "--fast-mode",
        "--output-partitions", "4",
        "--spark-parallelism", "8",
        "--shuffle-partitions", "8"
    )

Submit-PysparkBatch `
    -BatchName "chat-s2g-$safeDate-$stamp" `
    -MainPythonFileUri "gs://$Bucket/scripts/chat_pipeline/chat_silver_to_gold_spark.py" `
    -JobArgs @(
        "--tables", "all",
        "--fast-mode",
        "--gold-output-partitions", "4",
        "--spark-parallelism", "8",
        "--shuffle-partitions", "8"
    )

if ($DryRun) {
    Write-Host "Nightly dashboard refresh dry-run completed. No Dataproc batches were submitted. run_id=$runId process_date=$ProcessDate"
} else {
    Write-Host "Nightly dashboard refresh submitted. run_id=$runId process_date=$ProcessDate"
}
