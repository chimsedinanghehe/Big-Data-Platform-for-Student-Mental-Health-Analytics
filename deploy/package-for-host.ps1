param(
    [string]$OutputPath = "deploy/runtime/mindschool-app.tar.gz"
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$outputFullPath = Join-Path $repoRoot $OutputPath
$outputDir = Split-Path -Parent $outputFullPath
New-Item -ItemType Directory -Force $outputDir | Out-Null

if (Test-Path $outputFullPath) {
    Remove-Item $outputFullPath -Force
}

$excludes = @(
    "--exclude=.git",
    "--exclude=.venv",
    "--exclude=venv",
    "--exclude=__pycache__",
    "--exclude=*.pyc",
    "--exclude=frontend/node_modules",
    "--exclude=frontend/dist",
    "--exclude=MentalSchool_Dashboard/data/.dashboard_cache",
    "--exclude=deploy/runtime",
    "--exclude=deploy/backups",
    "--exclude=deploy/secrets",
    "--exclude=.env",
    "--exclude=.env.*",
    "--exclude=backend/.env",
    "--exclude=*.log",
    "--exclude=*.out",
    "--exclude=*.err",
    "--exclude=*.docx",
    "--exclude=*.pdf",
    "--exclude=*.zip",
    "--exclude=*.tar",
    "--exclude=*.tar.gz",
    "--exclude=~$*.docx"
)

Push-Location $repoRoot
try {
    tar @excludes -czf $outputFullPath .
}
finally {
    Pop-Location
}

Write-Host "Created $outputFullPath"
