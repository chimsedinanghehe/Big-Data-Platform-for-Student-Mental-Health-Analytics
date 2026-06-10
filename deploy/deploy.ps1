$ErrorActionPreference = "Stop"

$DeployRoot = Resolve-Path $PSScriptRoot
$EnvFile = Join-Path $DeployRoot ".env.production"
$ComposeFile = Join-Path $DeployRoot "docker-compose.production.yml"

& (Join-Path $DeployRoot "preflight.ps1")

if (docker compose --env-file $EnvFile -f $ComposeFile ps --status running --services | Select-String "^postgres$") {
    & (Join-Path $DeployRoot "backup-postgres.ps1")
}

docker compose --env-file $EnvFile -f $ComposeFile build --pull
if ($LASTEXITCODE -ne 0) { throw "Image build failed." }

docker compose --env-file $EnvFile -f $ComposeFile up -d --remove-orphans
if ($LASTEXITCODE -ne 0) { throw "Deployment failed." }

docker compose --env-file $EnvFile -f $ComposeFile ps
