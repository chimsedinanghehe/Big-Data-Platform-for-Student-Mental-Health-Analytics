$ErrorActionPreference = "Stop"

$DeployRoot = Resolve-Path $PSScriptRoot
$EnvFile = Join-Path $DeployRoot ".env.production"
$SecretFile = Join-Path $DeployRoot "secrets\gcp-service-account.json"
$ComposeFile = Join-Path $DeployRoot "docker-compose.production.yml"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker is not installed or not available on PATH."
}

if (-not (Test-Path $EnvFile)) {
    throw "Missing deploy\.env.production. Copy deploy\.env.production.example and fill production values."
}

if (-not (Test-Path $SecretFile)) {
    throw "Missing deploy\secrets\gcp-service-account.json."
}

$required = @(
    "APP_DOMAIN",
    "ACME_EMAIL",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "OPENAI_API_KEY",
    "QDRANT_URL",
    "GCP_PROJECT_ID",
    "GCS_BUCKET_NAME",
    "KAFKA_BOOTSTRAP_SERVERS"
)

$values = @{}
Get-Content $EnvFile | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
        return
    }
    $key, $value = $line.Split("=", 2)
    $values[$key.Trim()] = $value.Trim()
}

$missing = $required | Where-Object {
    -not $values.ContainsKey($_) -or
    -not $values[$_] -or
    $values[$_] -match "replace|example\.com"
}
if ($missing) {
    throw "Missing or placeholder production values: $($missing -join ', ')"
}

if ($values["KAFKA_BOOTSTRAP_SERVERS"] -match "^(127\.0\.0\.1|localhost)") {
    throw "KAFKA_BOOTSTRAP_SERVERS must be reachable from the deployment host/container."
}

docker compose --env-file $EnvFile -f $ComposeFile config --quiet
if ($LASTEXITCODE -ne 0) {
    throw "Docker Compose configuration validation failed."
}

Write-Host "Production preflight passed." -ForegroundColor Green
Write-Host "Domain: https://$($values['APP_DOMAIN'])"
Write-Host "Kafka: $($values['KAFKA_BOOTSTRAP_SERVERS'])"
