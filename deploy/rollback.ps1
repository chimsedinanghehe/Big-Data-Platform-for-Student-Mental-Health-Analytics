$ErrorActionPreference = "Stop"

$DeployRoot = Resolve-Path $PSScriptRoot
$EnvFile = Join-Path $DeployRoot ".env.production"
$ComposeFile = Join-Path $DeployRoot "docker-compose.production.yml"

docker compose --env-file $EnvFile -f $ComposeFile down
if ($LASTEXITCODE -ne 0) { throw "Rollback/stop failed." }

Write-Host "Application containers stopped. Persistent PostgreSQL and Caddy volumes were preserved." -ForegroundColor Yellow
