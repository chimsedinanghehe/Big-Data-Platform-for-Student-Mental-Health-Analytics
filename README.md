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

Pipeline hien tai khong dung version partition trong output path. Khi them tai lieu moi, co the chay incremental append cho mot hoac nhieu file. Khi sua/xoa tai lieu cu, chay `overwrite` de rebuild Silver, Gold va embedding artifact tu toan bo knowledge base.

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
scripts/preprocessing/pdf_to_silver.py
scripts/preprocessing/run_rag_preprocessing_dataproc.py
scripts/preprocessing/verify_silver_chunks.py

scripts/run_rag_pipeline.py
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

Pipeline runner la cach chay chinh. Runner khong doc file local de preprocess; tai lieu phai nam tren Bronze GCS truoc:

```text
gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/
```

Chay menu:

```powershell
venv\Scripts\python.exe scripts\run_rag_pipeline.py
```

Neu chua login Google Cloud tren may nay, login truoc:

```powershell
gcloud auth login
gcloud auth application-default login
gcloud config set project student-mental-health-496205
```

Dataproc dependency can co trong bucket. Neu chua co, upload mot lan:

```powershell
gcloud storage cp build\pypdf_deps.zip gs://student-mental-health-lake-nhom1-2026/jobs/deps/pypdf_deps.zip
```

The preprocessing script installs this zip with a Dataproc initialization action; it does not SSH into the cluster.

### Them tai lieu moi

Dung khi file moi da co tren Bronze va chua tung duoc index.

Neu them vai file le va biet exact GCS path, chon option `1`:

```text
1. Run incremental pipeline for exact Bronze GCS file path(s)
```

Nhap mot file:

```text
gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/new_document.pdf
```

Nhap nhieu file bang dau `;`:

```text
gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/a.pdf;gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/b.pdf
```

Neu upload ca batch vao mot folder rieng, chon option `2`:

```text
2. Run incremental pipeline for a Bronze GCS prefix/folder
```

Nhap prefix/folder:

```text
gs://student-mental-health-lake-nhom1-2026/bronze/knowledge_base/batch_20260530/
```

Option `1` va `2` se chay:

```text
Bronze new file(s)
-> Dataproc append Silver clean docs
-> Dataproc append Gold RAG chunks
-> generate embeddings cho file/prefix moi
-> merge vao vector/embeddings/student_mental_health_v1/embeddings.jsonl
-> hoi co upsert vao Qdrant student_mental_health_v1 khong
```

Chon `y` khi runner hoi upsert neu muon chatbot retrieval duoc tai lieu moi ngay.

### Sua, thay the, hoac xoa tai lieu cu

Khong dung option `1`/`2`, vi append mode khong xoa stale rows cu trong Silver/Gold. Dung option `6`:

```text
6. Full rebuild end-to-end
```

Option `6` se chay:

```text
Bronze full folder
-> overwrite Silver/Gold
-> regenerate embeddings.jsonl
-> hoi co upsert rebuilt chunks vao Qdrant khong
```

Chon `y` khi runner hoi upsert neu muon collection `student_mental_health_v1` cap nhat theo ban rebuild.

### Chay tung chang de kiem tra

Dung cac option nay neu muon tach pipeline thanh tung buoc:

```text
3. Full rebuild Silver/Gold tu toan bo Bronze
4. Save embedding artifact tu toan bo Gold
5. Upsert toan bo Gold vao Qdrant
```

Thu tu dung khi chay tach buoc:

```text
3 -> kiem tra Silver/Gold -> 4 -> kiem tra embeddings.jsonl -> 5
```

### Backup Qdrant truoc rebuild lon

Truoc khi rebuild lon, co the chon option `7`:

```text
7. Export Qdrant collection backup to GCS
```

Backup se ghi vao:

```text
gs://student-mental-health-lake-nhom1-2026/vector_backup/qdrant/student_mental_health_v1/export_before_rebuild_<timestamp>.jsonl
```

### Doi bucket/project/collection tam thoi

Chon option `8` neu can doi config trong phien menu hien tai:

```text
8. Change config for this run
```

Option nay khong sua file `.env` hay README.

Expected output paths:

```text
gs://student-mental-health-lake-nhom1-2026/silver/knowledge_base_clean/parquet/
gs://student-mental-health-lake-nhom1-2026/silver/knowledge_base_clean/jsonl/
gs://student-mental-health-lake-nhom1-2026/gold/rag_chunks/parquet/
gs://student-mental-health-lake-nhom1-2026/gold/rag_chunks/jsonl/
gs://student-mental-health-lake-nhom1-2026/vector/embeddings/student_mental_health_v1/embeddings.jsonl
```

## Manual Commands

Chi dung cac lenh duoi day khi muon debug, khong muon dung menu runner.

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

Generate embedding artifact for all Gold chunks, without writing Qdrant:

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

Khi them tai lieu moi:

1. Upload file vao `bronze/knowledge_base/`.
2. Chay runner option `1` cho exact file path(s), hoac option `2` cho dedicated Bronze prefix/folder.
3. Chon `y` khi runner hoi upsert neu muon chatbot retrieval duoc tai lieu moi ngay.

Khi thay the hoac xoa tai lieu cu:

1. Chay runner option `6`.
2. Chon `y` khi runner hoi upsert neu muon collection hien tai cap nhat theo rebuild.

Collection cu `student_mental_health` co mixed payload format nen khong nen tiep tuc ghi vao do cho rebuild moi. Dung `student_mental_health_v1` de index sach.

## Qdrant Backup

Truoc khi rebuild lon, cach khuyen dung la chay runner option `7`.

Lenh manual tuong duong:

```powershell
python scripts/embeddings/export_qdrant_to_gcs.py `
  --collection-name student_mental_health_v1 `
  --gcs-bucket-name student-mental-health-lake-nhom1-2026 `
  --gcs-prefix vector_backup/qdrant `
  --filename-prefix export_before_rebuild `
  --format jsonl
```

Backup path mong muon:

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

He thong nay khong phai he thong chan doan y te. Chatbot chi nen cung cap thong tin ho tro, bao ve rieng tu nguoi dung, va huong nguoi dung den ho tro chuyen mon khi can.
