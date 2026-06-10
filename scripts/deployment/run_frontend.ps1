<#
Run the chatbot frontend locally.

Usage from the project root:
  powershell -ExecutionPolicy Bypass -File scripts\deployment\run_frontend.ps1

What this does:
  - Runs Vite from frontend\
  - Uses npm.cmd to avoid PowerShell npm.ps1 execution-policy issues
  - Serves the UI at http://127.0.0.1:5173

Before running:
  - Run npm install once in frontend\, or let this script tell you if node_modules is missing
  - Start the backend separately with scripts\deployment\run_backend.ps1
#>

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$FrontendDir = Join-Path $ProjectRoot "frontend"
$PackageJson = Join-Path $FrontendDir "package.json"
$NodeModules = Join-Path $FrontendDir "node_modules"

if (-not (Test-Path $PackageJson)) {
    Write-Host "Missing frontend package.json: $PackageJson" -ForegroundColor Red
    exit 1
}

if (-not (Get-Command "node.exe" -ErrorAction SilentlyContinue)) {
    Write-Host "Node.js was not found on PATH." -ForegroundColor Red
    Write-Host "Install Node.js, then rerun this script." -ForegroundColor Yellow
    exit 1
}

if (-not (Get-Command "npm.cmd" -ErrorAction SilentlyContinue)) {
    Write-Host "npm.cmd was not found on PATH." -ForegroundColor Red
    exit 1
}

Set-Location $FrontendDir

if (-not (Test-Path $NodeModules)) {
    Write-Host "frontend\node_modules is missing. Installing frontend dependencies..." -ForegroundColor Yellow
    & npm.cmd install
}

Write-Host "Starting frontend..." -ForegroundColor Cyan
Write-Host "Open: http://127.0.0.1:5173"
Write-Host "Stop with Ctrl+C"

& npm.cmd run dev -- --port 5173
