# Big Data Platform for Student Mental Health Analytics

This repository contains a RAG chatbot and a Big Data knowledge-base preprocessing pipeline using Google Cloud Storage, Dataproc, Spark, embeddings, and Qdrant.

## Current Target

Cloud project:

```text
student-mental-health-496205
```

Main bucket:

```text
gs://student-mental-health-lake-nhom1-2026
```

Clean Qdrant collection for the current rebuild:

```text
student_mental_health_v1
```

## GCS Layout

The current pipeline does not use version partitions in output paths. For newly added documents, run the incremental append flow. For replaced, edited, or deleted documents, run a full overwrite rebuild so stale Silver/Gold rows are removed.

```text
gs://student-mental-health-lake-nhom1-2026/
  bronze/
    knowledge_base/                         # raw PDF/HTML/TXT/MD documents

  silver/
    knowledge_base_clean/
      parquet/                              # source of truth for Spark/pipeline jobs
      jsonl/                                # human-readable debug mirror
      incremental/
        run=<timestamp>/
          parquet/                          # append-only incremental clean docs
          jsonl/

  gold/
    rag_chunks/
      parquet/                              # standard input for embedding jobs
      jsonl/                                # human-readable chunk inspection mirror
      incremental/
        run=<timestamp>/
          parquet/                          # append-only incremental chunks
          jsonl/

  vector/
    embeddings/student_mental_health_v1/
      embeddings.jsonl                      # consolidated full-rebuild embedding backup
      manifest.json
      incremental/
        run=<timestamp>/
          embeddings.jsonl                  # append-only incremental embedding artifact
          manifest.json

  vector_backup/
    qdrant/student_mental_health_v1/
      export_before_rebuild_<timestamp>.jsonl

  scripts/
  jobs/deps/
  logs/dataproc/
```

## Pipeline

```text
Bronze knowledge_base
-> Dataproc Spark preprocessing
-> Silver clean documents
-> Gold RAG chunks
-> embedding artifact in GCS
-> optional Qdrant upsert to student_mental_health_v1
```

Supported Bronze document types:

```text
.pdf, .html, .htm, .txt, .md
```

## Scripts

```text
scripts/run_rag_pipeline.py

scripts/preprocessing/pdf_to_silver.py
scripts/preprocessing/run_rag_preprocessing_dataproc.py
scripts/preprocessing/verify_silver_chunks.py

scripts/embeddings/index_gold_rag_chunks_to_qdrant.py
scripts/embeddings/silver_to_qdrant.py
scripts/embeddings/export_qdrant_to_gcs.py
scripts/embeddings/test_qdrant_retrieval.py

scripts/deployment/run_all.bat
scripts/deployment/run_backend.bat
scripts/deployment/run_dashboard.bat
scripts/deployment/run_frontend.bat
scripts/deployment/run_postgres.bat
scripts/deployment/gcs_login.bat
```

## Run Pipeline

The pipeline runner is the primary entry point. It does not preprocess local files directly. Documents must already exist in Bronze GCS before running the pipeline:

```text
gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/
```

Start the menu:

```powershell
venv\Scripts\python.exe scripts\run_rag_pipeline.py
```

If this machine is not authenticated with Google Cloud yet, log in first:

```powershell
gcloud auth login
gcloud auth application-default login
gcloud config set project student-mental-health-496205
```

The Dataproc dependency package must exist in the bucket. Upload it once if it is missing:

```powershell
gcloud storage cp build\pypdf_deps.zip gs://student-mental-health-lake-nhom1-2026/jobs/deps/pypdf_deps.zip
```

The preprocessing runner installs this package with a Dataproc initialization action. It does not use SSH to install dependencies on the cluster.

### Add New Documents

Use this flow when the documents are new, already uploaded to Bronze, and have not been indexed before.

If you added a small number of files and know their exact GCS paths, choose option `1`:

```text
1. Run incremental pipeline for exact Bronze GCS file path(s)
```

Example for one file:

```text
gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/new_document.pdf
```

Example for multiple files, separated by semicolons:

```text
gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/a.pdf;gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/b.pdf
```

If you uploaded a batch into a dedicated Bronze folder, choose option `2`:

```text
2. Run incremental pipeline for a Bronze GCS prefix/folder
```

Example prefix:

```text
gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/batch_20260530/
```

Options `1` and `2` run:

```text
Bronze new file(s)
-> Dataproc append Silver clean docs under silver/knowledge_base_clean/incremental/run=<timestamp>/
-> Dataproc append Gold RAG chunks under gold/rag_chunks/incremental/run=<timestamp>/
-> generate embeddings from only that Gold run
-> write append-only embedding artifact under vector/embeddings/student_mental_health_v1/incremental/run=<timestamp>/
-> optionally upsert into Qdrant collection student_mental_health_v1 in the same embedding pass
```

Answer `y` to the upsert prompt if the chatbot should retrieve from the new documents immediately. Incremental runs do not scan the full Gold folder.

### Replace, Edit, Or Delete Documents

Do not use options `1` or `2` for replaced, edited, or deleted documents. Append mode cannot remove stale Silver/Gold rows. Use option `6` instead:

```text
6. Full rebuild end-to-end
```

Option `6` runs:

```text
Full Bronze folder
-> overwrite Silver/Gold
-> regenerate embeddings.jsonl
-> optionally upsert rebuilt chunks into Qdrant in the same embedding pass
```

Answer `y` to the upsert prompt if collection `student_mental_health_v1` should reflect the rebuilt data.

### Run Individual Stages

Use these options if you want to split the pipeline into manual checkpoints:

```text
3. Full rebuild Silver/Gold from all Bronze documents
4. Save embedding artifact from all Gold chunks
5. Upsert all Gold chunks to Qdrant
```

Recommended staged order:

```text
3 -> inspect Silver/Gold -> 4 -> inspect embeddings.jsonl -> 5
```

### Demo Without Waiting For Cluster Creation

For a presentation, warm the Dataproc cluster before the demo:

```text
9. Create/warm Dataproc cluster for demo
```

Then enable warmed-cluster mode if it is not already enabled:

```text
10. Use warmed Dataproc cluster for this session
```

When warmed-cluster mode is on, options `1`, `2`, `3`, and `6` pass:

```text
--skip-cluster-create --keep-cluster
```

so the pipeline submits jobs to the existing cluster and does not delete it after each run. When the presentation is done, delete it:

```text
11. Delete Dataproc cluster
```

This avoids cluster startup time during the demo while still letting you shut it down to avoid idle cost.

### Back Up Qdrant Before A Large Rebuild

Before a large rebuild, choose option `7`:

```text
7. Export Qdrant collection backup to GCS
```

Backup output:

```text
gs://student-mental-health-lake-nhom1-2026/vector_backup/qdrant/student_mental_health_v1/export_before_rebuild_<timestamp>.jsonl
```

### Change Runtime Config For One Menu Session

Choose option `8` to change the project, bucket, collection, chunk size, or chunk overlap for the current menu session:

```text
8. Change config for this run
```

This does not edit `.env`, README, or any source file.

Expected output paths:

```text
gs://student-mental-health-lake-nhom1-2026/silver/knowledge_base_clean/parquet/
gs://student-mental-health-lake-nhom1-2026/silver/knowledge_base_clean/jsonl/
gs://student-mental-health-lake-nhom1-2026/gold/rag_chunks/parquet/
gs://student-mental-health-lake-nhom1-2026/gold/rag_chunks/jsonl/
gs://student-mental-health-lake-nhom1-2026/silver/knowledge_base_clean/incremental/run=<timestamp>/parquet/
gs://student-mental-health-lake-nhom1-2026/gold/rag_chunks/incremental/run=<timestamp>/parquet/
gs://student-mental-health-lake-nhom1-2026/vector/embeddings/student_mental_health_v1/embeddings.jsonl
gs://student-mental-health-lake-nhom1-2026/vector/embeddings/student_mental_health_v1/incremental/run=<timestamp>/embeddings.jsonl
```

## Manual Commands

Use these commands only for debugging or when you intentionally do not want to use the menu runner.

Run preprocessing on Dataproc for the whole knowledge base:

```powershell
python scripts/preprocessing/run_rag_preprocessing_dataproc.py `
  --project=student-mental-health-496205 `
  --bucket=student-mental-health-lake-nhom1-2026 `
  --input_path=gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/ `
  --output-mode=overwrite `
  --chunk-size=500 `
  --chunk-overlap=100
```

Generate an embedding artifact for all Gold chunks without writing to Qdrant:

```powershell
venv\Scripts\python.exe scripts\embeddings\index_gold_rag_chunks_to_qdrant.py `
  --bucket=student-mental-health-lake-nhom1-2026 `
  --collection-name=student_mental_health_v1
```

Upsert all Gold chunks to Qdrant collection `student_mental_health_v1`:

```powershell
venv\Scripts\python.exe scripts\embeddings\index_gold_rag_chunks_to_qdrant.py `
  --bucket=student-mental-health-lake-nhom1-2026 `
  --collection-name=student_mental_health_v1 `
  --upsert-to-qdrant
```

Backend collection config:

```env
QDRANT_COLLECTION=student_mental_health_v1
GCS_BUCKET=student-mental-health-lake-nhom1-2026
GCS_BUCKET_NAME=student-mental-health-lake-nhom1-2026
GCS_KNOWLEDGE_BASE_PREFIX=bronze/knowledge_base
```

## Rebuild Policy

When adding new documents:

1. Upload the files to `bronze/knowledge_base/`.
2. Run option `1` for exact file path(s), or option `2` for a dedicated Bronze prefix/folder.
3. Answer `y` to the upsert prompt if the chatbot should retrieve from the new documents immediately. Silver, Gold, and embedding incremental artifacts are written under `incremental/run=<timestamp>`.

When replacing, editing, or deleting existing documents:

1. Run option `6`.
2. Answer `y` to the upsert prompt if the current collection should reflect the rebuild.

The legacy collection `student_mental_health` contains mixed payload formats. Do not write new rebuilds into it unless explicitly required. Use `student_mental_health_v1` for the clean index.

## Qdrant Backup

The recommended backup path is runner option `7`.

Equivalent manual command:

```powershell
python scripts/embeddings/export_qdrant_to_gcs.py `
  --collection-name student_mental_health_v1 `
  --gcs-bucket-name student-mental-health-lake-nhom1-2026 `
  --gcs-prefix vector_backup/qdrant `
  --filename-prefix export_before_rebuild `
  --format jsonl
```

Expected backup path:

```text
gs://student-mental-health-lake-nhom1-2026/vector_backup/qdrant/student_mental_health_v1/export_before_rebuild_<timestamp>.jsonl
```

## Local Unified App

```powershell
scripts\deployment\run_all.bat
```

This starts:

```text
PostgreSQL: 127.0.0.1:5433
Backend:    http://127.0.0.1:8000
Dashboard:  http://127.0.0.1:8501
Frontend:   http://127.0.0.1:5173
```

The frontend is now the shared app shell:

```text
Student role    -> Chatbot + Profile
Researcher role -> Dashboard + Profile
Profile/Login   -> PostgreSQL user tables
```

The PostgreSQL database is local Docker Compose storage and is only for account/profile metadata. It does not store chat messages, RAG chunks, embeddings, Qdrant data, or Spark outputs.

Manual startup order:

```powershell
scripts\deployment\run_postgres.bat
scripts\deployment\run_backend.bat
scripts\deployment\run_dashboard.bat
scripts\deployment\run_frontend.bat
```

Install dashboard and PostgreSQL client dependencies if needed:

```powershell
venv\Scripts\python.exe -m pip install -r requirements.txt
venv\Scripts\python.exe -m pip install -r MentalSchool_Dashboard\requirements.txt
```

PostgreSQL schema:

```text
backend/db/schema.sql
```

Current user tables:

```text
app_users(id, email, password_hash, display_name, role, is_active, created_at, updated_at)
student_profiles(user_id, age, gender, learner_type)
researcher_profiles(user_id)
app_sessions(id, user_id, token_hash, created_at, expires_at)
```

API:

```text
POST /api/auth/register
POST /api/auth/login
GET  /api/auth/me
PUT  /api/users/me
```

Supported roles:

```text
student
researcher
```

After login, navigation is role-based. Student accounts see only Chatbot and Profile. Researcher accounts see only Dashboard and Profile. Student profiles keep only the current research fields used by the app: age, gender, and learner type. Researcher accounts keep only account-level identity and role metadata.

Seed demo accounts:

```powershell
$env:DATABASE_URL="postgresql://student_app:student_app_password@127.0.0.1:5433/student_mental_health_app"
venv\Scripts\python.exe scripts\deployment\seed_demo_users.py
```

Demo logins:

```text
student.demo@example.com      / StudentDemo123!
highschool.demo@example.com   / StudentDemo123!
researcher.demo@example.com   / ResearcherDemo123!
```

## Safety

This project is not a medical diagnosis system. The chatbot should provide supportive information, protect user privacy, and direct users to professional or emergency support when needed.
