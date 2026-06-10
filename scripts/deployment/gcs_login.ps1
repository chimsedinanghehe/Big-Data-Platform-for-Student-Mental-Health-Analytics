<#
Authenticate and verify Google Cloud Storage access for this project.

Usage from project root:
  powershell -ExecutionPolicy Bypass -File scripts\deployment\gcs_login.ps1

Optional:
  powershell -ExecutionPolicy Bypass -File scripts\deployment\gcs_login.ps1 -ProjectId your-gcp-project-id
  powershell -ExecutionPolicy Bypass -File scripts\deployment\gcs_login.ps1 -SkipWriteTest

What this checks:
  - gcloud is installed
  - gsutil is installed
  - gcloud user login exists
  - Application Default Credentials exist for Python google-cloud-storage
  - bucket can be listed
  - a small test object can be written and deleted

Why both login types:
  - gsutil uses gcloud user credentials
  - Python google-cloud-storage often uses Application Default Credentials
#>

param(
    [string]$ProjectId = "",
    [switch]$SkipWriteTest
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$RootEnv = Join-Path $ProjectRoot ".env"

function Read-DotEnvValue {
    param(
        [string]$Path,
        [string]$Name
    )

    if (-not (Test-Path $Path)) {
        return ""
    }

    foreach ($line in Get-Content $Path) {
        if ($line -match "^\s*$Name\s*=\s*(.*)\s*$") {
            return $Matches[1].Trim().Trim('"').Trim("'")
        }
    }

    return ""
}

function Require-Command {
    param([string]$Name)

    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if (-not $command) {
        Write-Host "$Name was not found on PATH." -ForegroundColor Red
        Write-Host "Install Google Cloud SDK, then open a new terminal and rerun this script." -ForegroundColor Yellow
        exit 1
    }
}

Set-Location $ProjectRoot

$BucketName = Read-DotEnvValue -Path $RootEnv -Name "GCS_BUCKET_NAME"
if (-not $BucketName) {
    $BucketName = Read-DotEnvValue -Path $RootEnv -Name "GCS_BUCKET"
}

$ChatLogPrefix = Read-DotEnvValue -Path $RootEnv -Name "GCS_CHATLOG_PREFIX"
if (-not $ChatLogPrefix) {
    $ChatLogPrefix = "bronze/chat_logs"
}

if (-not $BucketName) {
    Write-Host "GCS_BUCKET_NAME or GCS_BUCKET is missing in .env." -ForegroundColor Red
    exit 1
}

Require-Command "gcloud.cmd"
Require-Command "gsutil.cmd"

Write-Host "Checking gcloud account..." -ForegroundColor Cyan
$ActiveAccount = (& gcloud.cmd auth list --filter=status:ACTIVE --format="value(account)") -join ""
if (-not $ActiveAccount) {
    Write-Host "No active gcloud account found. Opening browser login..." -ForegroundColor Yellow
    & gcloud.cmd auth login
    $ActiveAccount = (& gcloud.cmd auth list --filter=status:ACTIVE --format="value(account)") -join ""
}

if (-not $ActiveAccount) {
    Write-Host "gcloud login did not produce an active account." -ForegroundColor Red
    exit 1
}

Write-Host "Active account: $ActiveAccount" -ForegroundColor Green

if ($ProjectId) {
    Write-Host "Setting gcloud project: $ProjectId" -ForegroundColor Cyan
    & gcloud.cmd config set project $ProjectId | Out-Host
} else {
    $CurrentProject = (& gcloud.cmd config get-value project 2>$null) -join ""
    if ($CurrentProject) {
        Write-Host "Current gcloud project: $CurrentProject" -ForegroundColor Green
    } else {
        Write-Host "No gcloud project is configured. This is okay if bucket access works by IAM." -ForegroundColor Yellow
    }
}

Write-Host "Checking Application Default Credentials..." -ForegroundColor Cyan
$AdcOk = $true
try {
    & gcloud.cmd auth application-default print-access-token *> $null
} catch {
    $AdcOk = $false
}

if (-not $AdcOk) {
    Write-Host "Application Default Credentials are missing. Opening ADC login..." -ForegroundColor Yellow
    & gcloud.cmd auth application-default login
}

Write-Host "Checking bucket list access: gs://$BucketName" -ForegroundColor Cyan
& gsutil.cmd ls "gs://$BucketName/" | Select-Object -First 10 | Out-Host

if (-not $SkipWriteTest) {
    $ObjectName = "$($ChatLogPrefix.Trim('/'))/_auth_check/gcs_login_$([guid]::NewGuid().ToString('N')).jsonl"
    $TargetUri = "gs://$BucketName/$ObjectName"
    $Payload = "{`"event_type`":`"gcs_auth_check`",`"status`":`"ok`"}"

    Write-Host "Writing small test object: $TargetUri" -ForegroundColor Cyan
    $Payload | & gsutil.cmd cp - $TargetUri | Out-Host

    Write-Host "Deleting test object..." -ForegroundColor Cyan
    & gsutil.cmd rm $TargetUri | Out-Host
}

Write-Host "GCS auth check completed successfully." -ForegroundColor Green
