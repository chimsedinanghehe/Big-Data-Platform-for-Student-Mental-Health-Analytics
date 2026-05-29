# Run the Chatbot Locally

This guide runs the local FastAPI backend and the chatbot frontend on Windows.

## 1. Required Environment Files

Root `.env` should contain non-secret runtime config:

```env
QDRANT_URL=http://<your-qdrant-ip>:6333
QDRANT_COLLECTION=student_mental_health_v1
QDRANT_VECTOR_SIZE=384
GCS_BUCKET_NAME=student-mental-health-lake-nhom1-2026
GCS_BUCKET=student-mental-health-lake-nhom1-2026
GCS_KNOWLEDGE_BASE_PREFIX=bronze/knowledge_base
GCS_CHATLOG_PREFIX=bronze/chat_logs
OPENAI_MODEL=gpt-5.4-mini
```

Backend-only secret file:

```env
# backend/.env
OPENAI_API_KEY=your_openai_api_key_here
```

Do not put `OPENAI_API_KEY` in root `.env`.

## 2. Easiest Run Command

From the project root:

```powershell
scripts\deployment\run_all.bat
```

This opens two terminal windows:

- backend at `http://127.0.0.1:8000`
- frontend at `http://127.0.0.1:5173`

Open the chatbot UI:

```text
http://127.0.0.1:5173
```

## 3. Run Backend and Frontend Separately

Terminal 1:

```powershell
scripts\deployment\run_backend.bat
```

Terminal 2:

```powershell
scripts\deployment\run_frontend.bat
```

## 4. Health Checks

Backend:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

Expected:

```text
status
------
ok
```

Frontend:

```text
http://127.0.0.1:5173
```

## 5. GCS Login Check

If document ingest or chat-log upload fails because Google Cloud credentials are missing or expired, run:

```powershell
scripts\deployment\gcs_login.bat
```

If you need to set a specific GCP project:

```powershell
scripts\deployment\gcs_login.bat your-gcp-project-id
```

This checks `gcloud`, `gsutil`, user login, Application Default Credentials, bucket list access, and a small write/delete test object.

## 6. If the Page Looks Blank

Use a hard refresh:

```text
Ctrl + F5
```

If it is still blank, open browser DevTools with `F12`, go to `Console`, and check the first red error.

## 7. If Backend Port 8000 Is Already Used

Find the process:

```powershell
netstat -ano | findstr :8000
```

Stop it by PID:

```powershell
Stop-Process -Id <PID> -Force
```

Then rerun:

```powershell
scripts\deployment\run_backend.bat
```

## 8. If Frontend Port 5173 Is Already Used

Find the process:

```powershell
netstat -ano | findstr :5173
```

Stop it by PID:

```powershell
Stop-Process -Id <PID> -Force
```

Then rerun:

```powershell
scripts\deployment\run_frontend.bat
```

## 9. Notes

- Do not run `backend/rag/service.py` directly.
- Backend entrypoint is `backend.main:app`.
- Frontend entrypoint is Vite inside `frontend/`.
- Chat logs are uploaded to GCS under `bronze/chat_logs/date=YYYY-MM-DD/`.
- Session IDs are anonymized before writing logs.
- API responses and chat-log JSONL files do not include document source lists.
