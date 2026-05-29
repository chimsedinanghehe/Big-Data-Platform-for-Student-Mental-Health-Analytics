<# 
Run the FastAPI backend locally.

Usage from the project root:
  powershell -ExecutionPolicy Bypass -File scripts\deployment\run_backend.ps1

What this does:
  - Uses the project virtualenv: venv\Scripts\python.exe
  - Loads config from .env and backend\.env through backend code
  - Serves FastAPI at http://127.0.0.1:8000

Before running:
  - Put OPENAI_API_KEY only in backend\.env
  - Make sure QDRANT_URL in .env points to your running Qdrant server
  - If dependencies are missing, run: venv\Scripts\python.exe -m pip install -r requirements.txt
#>

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$Python = Join-Path $ProjectRoot "venv\Scripts\python.exe"
$BackendEnv = Join-Path $ProjectRoot "backend\.env"
$RootEnv = Join-Path $ProjectRoot ".env"

if (-not (Test-Path $Python)) {
    Write-Host "Missing virtualenv Python: $Python" -ForegroundColor Red
    Write-Host "Create/restore venv first, then install requirements." -ForegroundColor Yellow
    exit 1
}

if (-not (Test-Path $RootEnv)) {
    Write-Host "Missing root .env: $RootEnv" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $BackendEnv)) {
    Write-Host "Missing backend .env: $BackendEnv" -ForegroundColor Red
    Write-Host "Create backend\.env with OPENAI_API_KEY=your_key_here" -ForegroundColor Yellow
    exit 1
}

$hasOpenAIKey = Select-String -Path $BackendEnv -Pattern "^OPENAI_API_KEY=.+"
if (-not $hasOpenAIKey) {
    Write-Host "OPENAI_API_KEY is empty or missing in backend\.env" -ForegroundColor Yellow
    Write-Host "The UI can open, but chat generation will fail until this is set." -ForegroundColor Yellow
}

Set-Location $ProjectRoot

Write-Host "Starting FastAPI backend..." -ForegroundColor Cyan
Write-Host "Health check: http://127.0.0.1:8000/health"
Write-Host "Stop with Ctrl+C"

& $Python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000
