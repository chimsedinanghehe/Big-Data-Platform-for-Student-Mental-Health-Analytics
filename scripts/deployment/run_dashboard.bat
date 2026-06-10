@echo off
REM Run the Streamlit dashboard.
REM Usage from project root:
REM   scripts\deployment\run_dashboard.bat

cd /d "%~dp0..\.."
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\deployment\run_dashboard.ps1"

