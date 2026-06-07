$ErrorActionPreference = "Stop"

$DeployRoot = Resolve-Path $PSScriptRoot
$EnvFile = Join-Path $DeployRoot ".env.production"

if (-not (Test-Path $EnvFile)) {
    throw "Missing deploy\.env.production."
}

$domain = (
    Get-Content $EnvFile |
        Where-Object { $_ -match "^APP_DOMAIN=" } |
        Select-Object -Last 1
).Split("=", 2)[1].Trim()

$checks = @(
    "https://$domain/",
    "https://$domain/health",
    "https://$domain/ready",
    "https://$domain/dashboard/_stcore/health"
)

foreach ($url in $checks) {
    $response = Invoke-WebRequest -UseBasicParsing -Uri $url -TimeoutSec 20
    if ($response.StatusCode -lt 200 -or $response.StatusCode -ge 400) {
        throw "Verification failed: $url returned $($response.StatusCode)"
    }
    Write-Host "OK $url [$($response.StatusCode)]" -ForegroundColor Green
}
