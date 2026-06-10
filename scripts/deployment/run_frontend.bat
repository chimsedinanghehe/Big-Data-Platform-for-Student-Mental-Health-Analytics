@echo off
REM Run the chatbot frontend.
REM Usage from project root:
REM   scripts\deployment\run_frontend.bat
REM
REM Frontend URL:
REM   http://127.0.0.1:5173
REM
REM The backend should also be running at:
REM   http://127.0.0.1:8000

cd /d "%~dp0..\.."
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\deployment\run_frontend.ps1"
