# Big Data Platform for Student Mental Health Analytics

Repo nay chua chatbot RAG va pipeline Big Data xu ly knowledge base tren Google Cloud Storage bang Dataproc/Spark.

## Current Target

Cloud project:

```text
student-mental-health-496205
```

Main bucket:

```text
gs://student-mental-health-lake-nhom1-2026
```

Qdrant collection moi cho rebuild sach:

```text
student_mental_health_v1
```

## GCS Layout

Pipeline hien tai khong dung version partition trong output path. Moi lan them/sua tai lieu trong Bronze, chay lai pipeline voi `overwrite` de rebuild Silver, Gold va embedding artifact tu toan bo knowledge base.

```text
gs://student-mental-health-lake-nhom1-2026/
  bronze/
    knowledge_base/                         # PDF/HTML/TXT/MD raw documents

  silver/
    knowledge_base_clean/
      parquet/                              # source of truth cho Spark/pipeline
      jsonl/                                # de doc/debug bang gcloud/editor

  gold/
    rag_chunks/
      parquet/                              # input chuan cho embedding
      jsonl/                                # de kiem tra chunk_text

  vector/
    embeddings/student_mental_health_v1/
      embeddings.jsonl                      # backup embedding + payload

  vector_backup/
    qdrant/student_mental_health/
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

## Run Order: Add One New Document

Use this flow when you upload a new file and do not want to rebuild everything from scratch.

Set these variables in PowerShell:

```powershell
$Bucket = "student-mental-health-lake-nhom1-2026"
$Project = "student-mental-health-496205"
$Collection = "student_mental_health_v1"
$LocalFile = "D:\path\to\new_document.pdf"
$SourceFile = Split-Path $LocalFile -Leaf
$SourcePath = "gs://$Bucket/bronze/knowledge_base/$SourceFile"
```

1. Select the cloud project:

```powershell
gcloud config set project $Project
```

2. Upload the new document to Bronze:

```powershell
gcloud storage cp $LocalFile $SourcePath
```

3. Run Dataproc preprocessing for only that file, appending rows to Silver and Gold:

```powershell
python scripts/preprocessing/run_rag_preprocessing_dataproc.py `
  --project=$Project `
  --bucket=$Bucket `
  --input_path=$SourcePath `
  --output-mode=append `
  --chunk-size=500 `
  --chunk-overlap=100
```

4. Generate embeddings only for that source file and merge them into the main embedding artifact:

```powershell
venv\Scripts\python.exe scripts\embeddings\index_gold_rag_chunks_to_qdrant.py `
  --bucket=$Bucket `
  --collection-name=$Collection `
  --source-path=$SourcePath `
  --merge-embedding-output
```

5. Upsert only that source file into Qdrant:

```powershell
venv\Scripts\python.exe scripts\embeddings\index_gold_rag_chunks_to_qdrant.py `
  --bucket=$Bucket `
  --collection-name=$Collection `
  --source-path=$SourcePath `
  --no-save-embeddings `
  --upsert-to-qdrant
```

Expected paths after the incremental run:

```text
gs://student-mental-health-lake-nhom1-2026/silver/knowledge_base_clean/parquet/
gs://student-mental-health-lake-nhom1-2026/silver/knowledge_base_clean/jsonl/
gs://student-mental-health-lake-nhom1-2026/gold/rag_chunks/parquet/
gs://student-mental-health-lake-nhom1-2026/gold/rag_chunks/jsonl/
gs://student-mental-health-lake-nhom1-2026/vector/embeddings/student_mental_health_v1/embeddings.jsonl
```

For a truly new file, this avoids reprocessing and re-embedding the whole knowledge base. If you replace or delete an existing source file, use the full rebuild flow below so stale Silver/Gold rows are removed.

## Run Order: Full Rebuild

1. Login and select the cloud project:

```powershell
gcloud auth login
gcloud auth application-default login
gcloud config set project student-mental-health-496205
```

2. Upload Dataproc dependency if it is not already in the bucket:

```powershell
gcloud storage cp build\pypdf_deps.zip gs://student-mental-health-lake-nhom1-2026/jobs/deps/pypdf_deps.zip
```

The preprocessing script installs this zip with a Dataproc initialization action; it does not SSH into the cluster.

3. Upload or replace raw documents in Bronze:

```powershell
gcloud storage cp path\to\new_document.pdf gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/
```

4. Run preprocessing on Dataproc for the whole knowledge base:

```powershell
python scripts/preprocessing/run_rag_preprocessing_dataproc.py `
  --project=student-mental-health-496205 `
  --bucket=student-mental-health-lake-nhom1-2026 `
  --input_path=gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/ `
  --output-mode=overwrite `
  --chunk-size=500 `
  --chunk-overlap=100
```

Expected outputs:

```text
gs://student-mental-health-lake-nhom1-2026/silver/knowledge_base_clean/parquet/
gs://student-mental-health-lake-nhom1-2026/silver/knowledge_base_clean/jsonl/
gs://student-mental-health-lake-nhom1-2026/gold/rag_chunks/parquet/
gs://student-mental-health-lake-nhom1-2026/gold/rag_chunks/jsonl/
```

5. Generate embedding artifact for all Gold chunks, without writing Qdrant:

```powershell
venv\Scripts\python.exe scripts\embeddings\index_gold_rag_chunks_to_qdrant.py `
  --bucket=student-mental-health-lake-nhom1-2026 `
  --collection-name=student_mental_health_v1
```

Expected output:

```text
gs://student-mental-health-lake-nhom1-2026/vector/embeddings/student_mental_health_v1/embeddings.jsonl
```

6. Upsert all Gold chunks to Qdrant collection `student_mental_health_v1` after checking Gold chunks and embedding artifact:

```powershell
venv\Scripts\python.exe scripts\embeddings\index_gold_rag_chunks_to_qdrant.py `
  --bucket=student-mental-health-lake-nhom1-2026 `
  --collection-name=student_mental_health_v1 `
  --upsert-to-qdrant
```

7. Point backend to the new collection:

```env
QDRANT_COLLECTION=student_mental_health_v1
GCS_BUCKET=student-mental-health-lake-nhom1-2026
GCS_BUCKET_NAME=student-mental-health-lake-nhom1-2026
GCS_KNOWLEDGE_BASE_PREFIX=bronze/knowledge_base
```

## Rebuild Policy

Khi them 1 tai lieu moi:

1. Upload file vao `bronze/knowledge_base/`.
2. Chay incremental preprocessing voi `--input_path=<file_gcs_path>` va `--output-mode=append`.
3. Chay incremental embedding voi `--source-path=<file_gcs_path>` va `--merge-embedding-output`.
4. Upsert incremental vao `student_mental_health_v1` voi cung `--source-path`.

Khi thay the hoac xoa tai lieu cu:

1. Chay full rebuild voi `--output-mode=overwrite`.
2. Generate lai toan bo `embeddings.jsonl`.
3. Upsert lai collection sach hoac tao collection moi.

Collection cu `student_mental_health` co mixed payload format nen khong nen tiep tuc ghi vao do cho rebuild moi. Dung `student_mental_health_v1` de index sach.

## Qdrant Backup

Truoc khi rebuild lon, backup collection cu:

```powershell
python scripts/embeddings/export_qdrant_to_gcs.py `
  --collection-name student_mental_health `
  --gcs-bucket-name student-mental-health-lake-nhom1-2026 `
  --gcs-prefix vector_backup/qdrant `
  --filename-prefix export_before_rebuild `
  --format jsonl
```

Backup path mong muon:

```text
gs://student-mental-health-lake-nhom1-2026/vector_backup/qdrant/student_mental_health/export_before_rebuild_<timestamp>.jsonl
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

He thong nay khong phai he thong chan doan y te. Chatbot chi nen cung cap thong tin ho tro, bao ve rieng tu nguoi dung, va huong nguoi dung den ho tro chuyen mon khi can.
