@echo off
REM Authenticate and verify Google Cloud Storage access.
REM Usage from project root:
REM   scripts\deployment\gcs_login.bat
REM
REM Optional project:
REM   scripts\deployment\gcs_login.bat your-gcp-project-id
REM
REM This checks gcloud login, Application Default Credentials,
REM bucket list access, and a small write/delete test object.

cd /d "%~dp0..\.."

if "%~1"=="" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\deployment\gcs_login.ps1"
) else (
  powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\deployment\gcs_login.ps1" -ProjectId "%~1"
)
