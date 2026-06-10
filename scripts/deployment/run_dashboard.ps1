<#
Run the Streamlit analytics dashboard locally.

Usage from the project root:
  powershell -ExecutionPolicy Bypass -File scripts\deployment\run_dashboard.ps1

Dashboard URL:
  http://127.0.0.1:8501
#>

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$Python = Join-Path $ProjectRoot "venv\Scripts\python.exe"
$DashboardRoot = Join-Path $ProjectRoot "MentalSchool_Dashboard"

if (-not (Test-Path $Python)) {
    Write-Host "Missing virtualenv Python: $Python" -ForegroundColor Red
    exit 1
}

if (-not $DashboardRoot) {
    Write-Host "Missing dashboard folder. Checked:" -ForegroundColor Red
    foreach ($candidate in $DashboardCandidates) {
        Write-Host "  $candidate" -ForegroundColor Yellow
    }
    exit 1
}

Set-Location $DashboardRoot

$env:STREAMLIT_BROWSER_GATHER_USAGE_STATS = "false"
$env:STREAMLIT_SERVER_HEADLESS = "true"

Write-Host "Starting Streamlit dashboard..." -ForegroundColor Cyan
Write-Host "Dashboard: http://127.0.0.1:8501"
Write-Host "Stop with Ctrl+C"

"" | & $Python -m streamlit run app.py `
    --server.address 127.0.0.1 `
    --server.port 8501 `
    --server.headless true `
    --server.showEmailPrompt false `
    --server.enableCORS false `
    --server.enableXsrfProtection false
