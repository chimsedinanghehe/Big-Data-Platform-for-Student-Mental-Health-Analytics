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

  gold/
    rag_chunks/
      parquet/                              # standard input for embedding jobs
      jsonl/                                # human-readable chunk inspection mirror

  vector/
    embeddings/student_mental_health_v1/
      embeddings.jsonl                      # embedding + payload backup

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
scripts/deployment/run_frontend.bat
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
-> Dataproc append Silver clean docs
-> Dataproc append Gold RAG chunks
-> generate embeddings for the new source path(s) or prefix
-> merge into vector/embeddings/student_mental_health_v1/embeddings.jsonl
-> ask whether to upsert into Qdrant collection student_mental_health_v1
```

Answer `y` to the upsert prompt if the chatbot should retrieve from the new documents immediately.

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
-> ask whether to upsert rebuilt chunks into Qdrant
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
gs://student-mental-health-lake-nhom1-2026/vector/embeddings/student_mental_health_v1/embeddings.jsonl
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
3. Answer `y` to the upsert prompt if the chatbot should retrieve from the new documents immediately.

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

## Local Chatbot

```powershell
scripts\deployment\run_all.bat
```

Backend:

```text
http://127.0.0.1:8000
```

Frontend:

```text
http://127.0.0.1:5173
```

## Safety

This project is not a medical diagnosis system. The chatbot should provide supportive information, protect user privacy, and direct users to professional or emergency support when needed.
