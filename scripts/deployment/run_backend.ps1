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
  - Start PostgreSQL with scripts\deployment\run_postgres.bat if user profile storage is needed
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

if (-not $env:DATABASE_URL) {
    $env:DATABASE_URL = "postgresql://student_app:student_app_password@127.0.0.1:5433/student_mental_health_app"
}

$existingBackend = $null
try {
    $existingBackend = Invoke-RestMethod -Uri "http://127.0.0.1:8000/health" -TimeoutSec 2
} catch {
    $existingBackend = $null
}

if ($existingBackend -and $existingBackend.status -eq "ok") {
    Write-Host "FastAPI backend is already running at http://127.0.0.1:8000" -ForegroundColor Green
    Write-Host "Health check: http://127.0.0.1:8000/health"
    exit 0
}

$portOwner = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if ($portOwner) {
    $process = Get-Process -Id $portOwner.OwningProcess -ErrorAction SilentlyContinue
    $processName = if ($process) { $process.ProcessName } else { "unknown" }
    Write-Host "Port 8000 is already in use by PID $($portOwner.OwningProcess) ($processName)." -ForegroundColor Red
    Write-Host "Stop that process or run the backend on a different port." -ForegroundColor Yellow
    exit 1
}

Write-Host "Starting FastAPI backend..." -ForegroundColor Cyan
Write-Host "Health check: http://127.0.0.1:8000/health"
Write-Host "User database: PostgreSQL on 127.0.0.1:5433"
Write-Host "Stop with Ctrl+C"

& $Python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000
