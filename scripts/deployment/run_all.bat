@echo off
REM Open backend and frontend in two separate terminal windows.
REM Usage from project root:
REM   scripts\deployment\run_all.bat
REM
REM After both windows finish starting:
REM   Backend:  http://127.0.0.1:8000/health
REM   Frontend: http://127.0.0.1:5173
REM
REM Stop:
REM   Press Ctrl+C in each terminal window.

cd /d "%~dp0..\.."

start "Student Mental Health Backend" cmd /k scripts\deployment\run_backend.bat
start "Student Mental Health Frontend" cmd /k scripts\deployment\run_frontend.bat

echo Started backend and frontend in separate windows.
echo Open http://127.0.0.1:5173 in your browser.
