$ErrorActionPreference = "Stop"

$DeployRoot = Resolve-Path $PSScriptRoot
$EnvFile = Join-Path $DeployRoot ".env.production"
$ComposeFile = Join-Path $DeployRoot "docker-compose.production.yml"
$BackupDir = Join-Path $DeployRoot "backups"

New-Item -ItemType Directory -Force $BackupDir | Out-Null
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$output = Join-Path $BackupDir "postgres-$stamp.sql"

docker compose --env-file $EnvFile -f $ComposeFile exec -T postgres `
    sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB"' |
    Set-Content -LiteralPath $output -Encoding utf8

if ($LASTEXITCODE -ne 0) {
    Remove-Item -LiteralPath $output -Force -ErrorAction SilentlyContinue
    throw "PostgreSQL backup failed."
}

Compress-Archive -LiteralPath $output -DestinationPath "$output.zip" -Force
Remove-Item -LiteralPath $output -Force
Write-Host "Created $output.zip" -ForegroundColor Green
