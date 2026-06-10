@echo off
REM Open backend, dashboard, and frontend in separate terminal windows.
REM Usage from project root:
REM   scripts\deployment\run_all.bat
REM
REM After both windows finish starting:
REM   Backend:   http://127.0.0.1:8000/health
REM   Dashboard: http://127.0.0.1:8501
REM   Frontend:  http://127.0.0.1:5173
REM
REM Stop:
REM   Press Ctrl+C in each terminal window.

cd /d "%~dp0..\.."

call scripts\deployment\run_postgres.bat
start "Student Mental Health Backend" cmd /k scripts\deployment\run_backend.bat
start "Student Mental Health Dashboard" cmd /k scripts\deployment\run_dashboard.bat
start "Student Mental Health Frontend" cmd /k scripts\deployment\run_frontend.bat

echo Started backend, dashboard, and frontend in separate windows.
echo Open http://127.0.0.1:5173 in your browser.
