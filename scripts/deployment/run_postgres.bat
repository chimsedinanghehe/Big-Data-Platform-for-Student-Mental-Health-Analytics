@echo off
REM Start local PostgreSQL for user profile metadata.
REM Usage from project root:
REM   scripts\deployment\run_postgres.bat

cd /d "%~dp0..\.."

where docker >nul 2>nul
if errorlevel 1 (
    echo Docker was not found. Install Docker Desktop or start PostgreSQL manually.
    exit /b 1
)

docker compose up -d postgres
if errorlevel 1 (
    echo Failed to start PostgreSQL with docker compose.
    exit /b 1
)

echo Waiting for PostgreSQL to accept connections...
for /l %%i in (1,1,30) do (
    docker exec student-mental-health-postgres pg_isready -U student_app -d student_mental_health_app >nul 2>nul
    if not errorlevel 1 (
        echo PostgreSQL is ready on localhost:5433.
        echo Backend launcher will use the matching local DATABASE_URL.
        exit /b 0
    )
    timeout /t 2 /nobreak >nul
)

echo PostgreSQL did not become ready within 60 seconds.
exit /b 1
