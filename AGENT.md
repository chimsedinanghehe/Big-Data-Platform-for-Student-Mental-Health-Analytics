# AGENT.md

## Project

Big Data Platform for Student Mental Health Analytics and RAG-based Agent.

This file is the project source of truth for Codex/GPT work in this repo. Read it before changing code, running cloud jobs, or explaining architecture.

## Current Phase

The project currently has two active tracks:

1. RAG chatbot runtime
   - FastAPI backend runs locally.
   - Unified frontend app runs locally with Vite.
   - Chatbot UI is one tab in the unified frontend.
   - Streamlit analytics dashboard is embedded as another tab.
   - PostgreSQL stores only application account/profile metadata.
   - Supported app roles are `student` and `researcher`.
   - Qdrant is the online vector database.
   - Qdrant is self-hosted on a GCP VM.
   - OpenAI Responses API is the default generation backend.
   - Chat logs are uploaded to GCS as anonymized JSONL.

2. Big Data preprocessing pipeline
   - PDF/HTML/TXT/MD raw documents are stored in GCS Bronze.
   - Dataproc runs Spark jobs on GCS data.
   - Spark extracts and cleans text, then writes clean document pages to GCS Silver.
   - Spark writes RAG-ready chunks to GCS Gold.
   - Gold RAG chunks are the intended input for embedding artifacts and optional Qdrant upsert.

Do not run Spark preprocessing locally when the user asks to process on GCS/Dataproc.

## High-Level Architecture

```text
Knowledge base documents
-> GCS Bronze layer
-> Dataproc Spark preprocessing
-> GCS Silver layer as clean document pages
-> GCS Gold layer as RAG chunks
-> embedding job
-> GCS vector/embeddings artifacts
-> Qdrant collection
-> FastAPI RAG backend
-> OpenAI Responses API
-> unified frontend chatbot tab
-> anonymized chat logs back to GCS
```

Qdrant remains the realtime vector search engine. Google Cloud Storage is used for raw files, processed data, logs, model/data artifacts, and vector backups.

## Current Cloud State

Current working GCS bucket:

```text
gs://student-mental-health-lake-nhom1-2026
```

Current GCP project used for Dataproc preprocessing:

```text
student-mental-health-496205
```

Current region/zone used:

```text
Region: asia-southeast1
Zone:   asia-southeast1-a
```

Dataproc smoke test on this project and bucket passed on 2026-05-28. Requester Pays is off, and the default compute service account can run Dataproc jobs.

## Data Lake Layout

Recommended bucket layout:

```text
gs://student-mental-health-lake-nhom1-2026/
  bronze/
    knowledge_base/
    chat_logs/date=YYYY-MM-DD/
  silver/
    knowledge_base_clean/
      parquet/
      jsonl/
      incremental/run=<timestamp>/parquet/
      incremental/run=<timestamp>/jsonl/
  gold/
    rag_chunks/
      parquet/
      jsonl/
      incremental/run=<timestamp>/parquet/
      incremental/run=<timestamp>/jsonl/
  vector/
    embeddings/student_mental_health_v1/
      embeddings.jsonl
      manifest.json
      incremental/run=<timestamp>/embeddings.jsonl
      incremental/run=<timestamp>/manifest.json
  vector_backup/
    qdrant/student_mental_health/export_before_rebuild_<timestamp>.jsonl
  scripts/
  jobs/deps/
  logs/dataproc/
```

Parquet is the source-of-truth format for Spark jobs. JSONL mirrors are written for easier manual inspection.

## Dataproc Preprocessing

Primary Spark job:

```text
scripts/preprocessing/pdf_to_silver.py
```

Support scripts:

```text
scripts/run_rag_pipeline.py
scripts/preprocessing/run_rag_preprocessing_dataproc.py
scripts/preprocessing/verify_silver_chunks.py
scripts/embeddings/index_gold_rag_chunks_to_qdrant.py
scripts/embeddings/silver_to_qdrant.py
scripts/preprocessing/dataproc_preprocess_knowledgebase.py
```

The main job reads files from GCS using Spark `binaryFile`, extracts text from `.pdf`, `.html`, `.htm`, `.txt`, and `.md`, cleans text, and writes Parquet plus JSONL.

Default output is page-level:

```text
document_id
source_path
source_file
file_hash
file_size
page_number
clean_text
text_length
language
processed_at
status
error
```

With `--emit_chunks`, output is chunk-level:

```text
document_id
source_path
source_file
file_hash
file_size
page_number
chunk_id
chunk_index
chunk_text
chunk_text_length
chunk_size
chunk_overlap
language
processed_at
status
error
```

Current target output paths:

```text
Clean documents: gs://student-mental-health-lake-nhom1-2026/silver/knowledge_base_clean/
RAG chunks:      gs://student-mental-health-lake-nhom1-2026/gold/rag_chunks/
Embeddings:      gs://student-mental-health-lake-nhom1-2026/vector/embeddings/student_mental_health_v1/embeddings.jsonl
```

Operational rules:

- Prefer temporary Dataproc clusters and delete them after the job.
- For presentations or repeated incremental demos, use the runner's warmed-cluster flow: create the cluster first, run preprocessing with `--skip-cluster-create --keep-cluster`, then delete the cluster when done.
- Package `pypdf` as `gs://student-mental-health-lake-nhom1-2026/jobs/deps/pypdf_deps.zip`; `run_rag_preprocessing_dataproc.py` installs it with a Dataproc initialization action.
- Do not rely on `gcloud compute ssh` for installing dependencies; the current user may not have `compute.instances.get`.
- Full rebuild output is unversioned. Incremental output should use `incremental/run=<timestamp>` under both Silver and Gold.
- Do not write new rebuilds into the old mixed-format Qdrant collection unless explicitly requested.
- For one or more newly uploaded source files already in Bronze GCS, use incremental mode: preprocessing with `--version=incremental/run=<timestamp>` and `--output-mode=append`, then index from `gold/rag_chunks/incremental/run=<timestamp>/parquet/`.
- For a dedicated Bronze batch folder, preprocessing may use the folder prefix with `--version=incremental/run=<timestamp>` and `--output-mode=append`, then index from the matching Gold incremental run.
- For replaced or deleted source files, run a full rebuild with `--output-mode=overwrite`; append mode cannot remove stale Silver/Gold rows.
- If the user wants a short command or interactive choice, use `venv\Scripts\python.exe scripts\run_rag_pipeline.py`.

## Gold Chunks To Embedding / Qdrant Flow

When embeddings should be generated from Gold GCS data, the intended flow is:

```text
Read gs://student-mental-health-lake-nhom1-2026/gold/rag_chunks/parquet/
-> filter status = ok and chunk_text is not empty
-> generate embedding from chunk_text
-> save embedding JSONL to gs://student-mental-health-lake-nhom1-2026/vector/embeddings/student_mental_health_v1/embeddings.jsonl
-> optionally upsert into Qdrant with --upsert-to-qdrant
-> store metadata in Qdrant payload
-> query Qdrant from the RAG backend
```

Recommended Qdrant point shape:

```json
{
  "id": "<chunk_id>",
  "vector": [0.1, 0.2],
  "payload": {
    "chunk_text": "...",
    "document_id": "...",
    "source_path": "gs://...",
    "source_file": "...",
    "page": 1,
    "chunk_index": 0,
    "language": "en",
    "processed_at": "..."
  }
}
```

Do not treat GCS as a similarity search database. GCS stores data and backups; Qdrant performs vector indexing and search.

For incremental embedding runs, prefer append-only run artifacts instead of merging into the consolidated `embeddings.jsonl`:

```text
gs://student-mental-health-lake-nhom1-2026/vector/embeddings/student_mental_health_v1/incremental/run=<timestamp>/embeddings.jsonl
gs://student-mental-health-lake-nhom1-2026/vector/embeddings/student_mental_health_v1/incremental/run=<timestamp>/manifest.json
```

The consolidated `embeddings.jsonl` should be regenerated by full rebuilds. This avoids reading and rewriting a growing JSONL file on every incremental update.

## Qdrant

Recommended clean rebuild collection:

```text
student_mental_health_v1
```

Existing legacy collection:

```text
student_mental_health
```

The legacy collection contains mixed payload formats (`page_content`/`metadata` and `chunk_text` metadata). Prefer `student_mental_health_v1` for the clean rebuild.

Embedding model:

```text
sentence-transformers/all-MiniLM-L6-v2
```

Vector dimension:

```text
384
```

Distance:

```text
Cosine
```

Do not switch back to MongoDB Atlas unless the user explicitly asks.

## Qdrant Backup To GCS

Script:

```text
scripts/embeddings/export_qdrant_to_gcs.py
```

The script scrolls the whole Qdrant collection in batches, exports JSONL or Parquet, and uploads to:

```text
gs://<bucket>/vector_backup/qdrant/<collection_name>/export_<timestamp>.jsonl
```

For pre-rebuild backups, use `--filename-prefix export_before_rebuild` so the object name matches:

```text
gs://student-mental-health-lake-nhom1-2026/vector_backup/qdrant/student_mental_health/export_before_rebuild_<timestamp>.jsonl
```

Known previous export:

```text
Collection: student_mental_health
Rows:       11717 points
Format:     JSONL
Size:       about 80.8 MB
```

## RAG Runtime Flow

```text
Unified frontend Chat tab
-> FastAPI backend
-> query rewrite
-> embeddings
-> Qdrant semantic retrieval
-> OpenAI Responses API
-> response returned to UI
-> anonymized JSONL chat log uploaded to GCS
```

Main local URLs:

```text
PostgreSQL: 127.0.0.1:5433
Backend:    http://127.0.0.1:8000
Health:     http://127.0.0.1:8000/health
Dashboard:  http://127.0.0.1:8501
Frontend:   http://127.0.0.1:5173
```

Run local unified app:

```powershell
scripts\deployment\run_all.bat
```

Run PostgreSQL only:

```powershell
scripts\deployment\run_postgres.bat
```

Run backend only:

```powershell
scripts\deployment\run_backend.bat
```

Run Streamlit dashboard only:

```powershell
scripts\deployment\run_dashboard.bat
```

Run frontend only:

```powershell
scripts\deployment\run_frontend.bat
```

Do not run `backend/rag/service.py` directly. Backend entrypoint is `backend.main:app`.

The user database schema is in `backend/db/schema.sql`. It creates:

```text
app_users
student_profiles
researcher_profiles
app_sessions
```

Rules:

- Store account/profile metadata only.
- Keep student research attributes limited to `age`, `gender`, and `learner_type`.
- Keep researcher metadata at account level only unless the user explicitly asks to add researcher-specific fields.
- Student accounts can access only Chatbot and Profile.
- Researcher accounts can access only Dashboard and Profile.
- Do not store chat text, RAG chunks, embeddings, Qdrant data, or Spark outputs in PostgreSQL unless the architecture is explicitly changed.

Demo accounts can be seeded with:

```powershell
venv\Scripts\python.exe scripts\deployment\seed_demo_users.py
```

## Environment Files

Root `.env` stores non-secret local runtime config.

Expected values:

```env
QDRANT_URL=http://<qdrant-vm-ip>:6333
QDRANT_COLLECTION=student_mental_health_v1
QDRANT_VECTOR_SIZE=384
RAG_EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
GCS_BUCKET=student-mental-health-lake-nhom1-2026
GCS_BUCKET_NAME=student-mental-health-lake-nhom1-2026
GCS_KNOWLEDGE_BASE_PREFIX=bronze/knowledge_base
GCS_CHATLOG_PREFIX=bronze/chat_logs
OPENAI_MODEL=gpt-5.4-mini
```

Backend-only secret file:

```env
# backend/.env
OPENAI_API_KEY=<secret>
```

Rules:

- Keep `OPENAI_API_KEY` only in `backend/.env`.
- Do not put `OPENAI_API_KEY` in frontend code.
- Do not commit `.env` files.
- Never hardcode secrets in source code.

## GCS Auth

If GCS commands fail because login is missing or expired, run:

```powershell
scripts\deployment\gcs_login.bat
```

The script checks:

- `gcloud`
- `gsutil`
- active gcloud account
- Application Default Credentials for Python clients
- bucket access
- optional small write/delete test under `bronze/chat_logs/_auth_check/`

## RAG API

Current route:

```text
POST /api/rag/ask
```

Request:

```json
{
  "question": "How can I manage stress?",
  "session_id": "browser-session-id",
  "chat_history": [
    {
      "role": "user",
      "content": "previous message"
    }
  ]
}
```

Response:

```json
{
  "answer": "...",
  "session_id": "browser-session-id"
}
```

The API must not return document sources to the frontend. The frontend must not display document sources.

## Safety Principle

This system is not a diagnosis system and must not claim to replace professional mental-health care.

The assistant should:

- provide supportive, practical information
- avoid diagnosis or clinical certainty
- encourage professional/campus support when appropriate
- include crisis guidance for immediate danger or self-harm signals
- preserve privacy and anonymity

## Privacy Rules

The project handles mental-health related text.

Rules:

- Use anonymous session IDs.
- Hash browser session IDs before writing logs.
- Mask obvious PII before persistence where possible.
- Do not store real student IDs unless explicitly required and protected.
- Do not index user chat text into Qdrant unless there is a specific reviewed design.
- Do not commit chat logs.
- Do not commit API keys or private credentials.

## Git Rules

Allowed to commit:

```text
backend/
frontend/
scripts/
docs/
configs/
README.md
AGENT.md
requirements.txt
```

Do not commit:

```text
.env
backend/.env
data/raw/
data/bronze/
data/silver/
data/gold/
data/processed/
data/embeddings/
logs/
frontend/node_modules/
frontend/dist/
private keys
API keys
chat logs containing sensitive data
Qdrant snapshots containing sensitive data
```

## Assistant Operating Instructions

When helping with this repo:

1. Read `AGENT.md` first.
2. Prefer one practical implementation path.
3. Use Windows PowerShell commands locally.
4. Use Linux commands only for the GCP VM or Dataproc nodes.
5. Use Dataproc for Spark jobs when the user asks to process data on GCS.
6. Keep Qdrant as the vector database.
7. Keep GCS as storage for raw data, clean data, logs, artifacts, and backups.
8. Keep OpenAI `gpt-5.4-mini` as the default response model unless the user changes it.
9. Keep `OPENAI_API_KEY` only in `backend/.env`.
10. Do not suggest MongoDB Atlas unless the user asks to compare or migrate.
11. Preserve privacy and anonymity.
12. Avoid clinical overclaiming.
13. Do not hardcode secrets.
14. Do not commit generated chat logs or local data.

## Next Useful Milestones

Recommended next steps:

1. Upload new documents to `gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/`.
2. For a new single file, run `scripts/preprocessing/run_rag_preprocessing_dataproc.py --input_path=<exact_gcs_file> --output-mode=append`.
3. Run `scripts/embeddings/index_gold_rag_chunks_to_qdrant.py --input_path=gs://<bucket>/gold/rag_chunks/incremental/run=<timestamp>/parquet/ --embedding-artifact-mode=run --run-id=run=<timestamp> --upsert-to-qdrant` to write the append-only embedding artifact and publish that run to Qdrant in one pass.
3. Add raw-to-silver chat-log processing.
4. Add daily Gold aggregate generation.
5. Add sentiment/emotion/risk tagging.
6. Add management dashboard after Silver/Gold outputs exist.
