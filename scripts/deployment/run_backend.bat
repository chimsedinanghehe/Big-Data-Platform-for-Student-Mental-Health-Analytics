@echo off
REM Run the FastAPI backend.
REM Usage from project root:
REM   scripts\deployment\run_backend.bat
REM
REM Backend URL:
REM   http://127.0.0.1:8000
REM
REM Required config:
REM   - backend\.env must contain OPENAI_API_KEY=...
REM   - .env must contain QDRANT_URL=...

cd /d "%~dp0..\.."
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\deployment\run_backend.ps1"
