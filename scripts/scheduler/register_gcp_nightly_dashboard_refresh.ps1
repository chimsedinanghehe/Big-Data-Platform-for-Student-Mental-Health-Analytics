param(
    [string]$ProjectId = "student-mental-health-496205",
    [string]$Region = "asia-southeast1",
    [string]$Bucket = "student-mental-health-lake-nhom1-2026",
    [string]$WorkflowName = "nightly-dashboard-refresh",
    [string]$SchedulerName = "nightly-dashboard-refresh-2330-vn",
    [string]$ServiceAccountEmail = "",
    [string]$GcloudPath = "gcloud",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$WorkflowFile = Join-Path $ProjectRoot "deploy\gcp\workflows\nightly_dashboard_refresh.yaml"

if (-not (Test-Path $WorkflowFile)) {
    throw "Missing workflow file: $WorkflowFile"
}

if (-not $ServiceAccountEmail) {
    $ServiceAccountEmail = "dashboard-refresh@$ProjectId.iam.gserviceaccount.com"
}

$deployWorkflow = @(
    $GcloudPath, "workflows", "deploy", $WorkflowName,
    "--project=$ProjectId",
    "--location=$Region",
    "--source=$WorkflowFile",
    "--service-account=$ServiceAccountEmail"
)

$body = "{`"project_id`":`"$ProjectId`",`"region`":`"$Region`",`"bucket`":`"$Bucket`"}"
$createScheduler = @(
    $GcloudPath, "scheduler", "jobs", "create", "http", $SchedulerName,
    "--project=$ProjectId",
    "--location=$Region",
    "--schedule=30 23 * * *",
    "--time-zone=Asia/Ho_Chi_Minh",
    "--uri=https://workflowexecutions.googleapis.com/v1/projects/$ProjectId/locations/$Region/workflows/$WorkflowName/executions",
    "--http-method=POST",
    "--oauth-service-account-email=$ServiceAccountEmail",
    "--headers=Content-Type=application/json",
    "--message-body=$body"
)

if ($DryRun) {
    Write-Host "[DRY-RUN] $($deployWorkflow -join ' ')"
    Write-Host "[DRY-RUN] $($createScheduler -join ' ')"
    exit 0
}

& $deployWorkflow[0] @($deployWorkflow[1..($deployWorkflow.Count - 1)])
if ($LASTEXITCODE -ne 0) {
    throw "Workflow deploy failed."
}

& $createScheduler[0] @($createScheduler[1..($createScheduler.Count - 1)])
if ($LASTEXITCODE -ne 0) {
    throw "Cloud Scheduler registration failed. If the job already exists, update it manually or delete/recreate it."
}

Write-Host "Registered Cloud Scheduler job $SchedulerName at 23:30 Asia/Ho_Chi_Minh."
