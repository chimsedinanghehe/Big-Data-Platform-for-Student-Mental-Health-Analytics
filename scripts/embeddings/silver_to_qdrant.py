from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

from google.api_core import exceptions as google_exceptions
from google.cloud import storage
import pyarrow.parquet as pq
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer


DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_COLLECTION = "student_mental_health_v1"
DEFAULT_VECTOR_SIZE = 384


@dataclass(frozen=True)
class Settings:
    input_path: str
    embedding_output_path: str | None
    qdrant_url: str
    qdrant_api_key: str | None
    collection_name: str
    embedding_model: str
    vector_size: int
    batch_size: int
    limit: int | None
    source_path: str | None
    source_prefix: str | None
    source_file: str | None
    dry_run: bool
    save_embeddings: bool
    merge_embedding_output: bool
    upsert_to_qdrant: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Embed Gold RAG chunks, optionally upsert them into Qdrant, and save embeddings to GCS.")
    parser.add_argument("--input_path", required=True, help="GCS Gold RAG chunk Parquet path, for example gs://bucket/gold/rag_chunks/parquet/")
    parser.add_argument(
        "--embedding-output-path",
        default=os.getenv("EMBEDDING_OUTPUT_PATH"),
        help="Optional GCS JSONL file path for embedding artifacts, for example gs://bucket/vector/embeddings/collection/embeddings.jsonl",
    )
    parser.add_argument("--qdrant-url", default=os.getenv("QDRANT_URL"))
    parser.add_argument("--qdrant-api-key", default=os.getenv("QDRANT_API_KEY"))
    parser.add_argument("--collection-name", default=os.getenv("QDRANT_COLLECTION", DEFAULT_COLLECTION))
    parser.add_argument("--embedding-model", default=os.getenv("RAG_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL))
    parser.add_argument("--vector-size", type=int, default=int(os.getenv("QDRANT_VECTOR_SIZE", DEFAULT_VECTOR_SIZE)))
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum valid chunks to process.")
    parser.add_argument("--source-path", default=None, help="Only process chunks from this exact source_path.")
    parser.add_argument("--source-prefix", default=None, help="Only process chunks whose source_path starts with this prefix.")
    parser.add_argument("--source-file", default=None, help="Only process chunks from this exact source_file.")
    parser.add_argument("--dry-run", action="store_true", help="Read and embed validation only; do not upsert.")
    parser.add_argument("--no-save-embeddings", action="store_true", help="Do not save generated embeddings to GCS.")
    parser.add_argument("--merge-embedding-output", action="store_true", help="Merge new embedding records into an existing JSONL artifact by id.")
    parser.add_argument("--skip-qdrant-upsert", action="store_true", help="Generate and save embeddings without writing points to Qdrant.")
    return parser.parse_args()


def main() -> int:
    setup_logging()
    load_env_files()
    args = parse_args()

    settings = Settings(
        input_path=args.input_path,
        embedding_output_path=args.embedding_output_path,
        qdrant_url=args.qdrant_url or "",
        qdrant_api_key=args.qdrant_api_key,
        collection_name=args.collection_name,
        embedding_model=args.embedding_model,
        vector_size=args.vector_size,
        batch_size=args.batch_size,
        limit=args.limit,
        source_path=args.source_path,
        source_prefix=args.source_prefix,
        source_file=args.source_file,
        dry_run=args.dry_run,
        save_embeddings=not args.no_save_embeddings,
        merge_embedding_output=args.merge_embedding_output,
        upsert_to_qdrant=not args.skip_qdrant_upsert,
    )
    if settings.upsert_to_qdrant and not settings.dry_run:
        require_value(settings.qdrant_url, "--qdrant-url or QDRANT_URL")

    with tempfile.TemporaryDirectory(prefix="gold_rag_chunks_") as temp_dir:
        parquet_files = download_parquet_parts(settings.input_path, Path(temp_dir))
        if not parquet_files:
            raise RuntimeError(f"No Parquet part files found under {settings.input_path}")

        client = None
        if settings.upsert_to_qdrant and not settings.dry_run:
            client = QdrantClient(url=settings.qdrant_url, api_key=settings.qdrant_api_key)
            ensure_collection(client, settings.collection_name, settings.vector_size)

        model = SentenceTransformer(settings.embedding_model)
        total_rows = 0
        valid_rows = 0
        upserted = 0
        embedding_jsonl = None
        embedding_record_count = 0
        embedding_record_ids: set[str] = set()

        if settings.save_embeddings and settings.embedding_output_path and not settings.dry_run:
            embedding_jsonl = Path(temp_dir) / "embeddings.jsonl"

        for batch in iter_valid_batches(
            parquet_files,
            settings.batch_size,
            source_path=settings.source_path,
            source_prefix=settings.source_prefix,
            source_file=settings.source_file,
        ):
            total_rows += batch["total_rows"]
            records = batch["records"]
            if not records:
                continue

            if settings.limit is not None:
                remaining = settings.limit - valid_rows
                if remaining <= 0:
                    break
                records = records[:remaining]

            valid_rows += len(records)
            texts = [record["chunk_text"] for record in records]
            embeddings = model.encode(
                texts,
                batch_size=settings.batch_size,
                normalize_embeddings=True,
                show_progress_bar=False,
            )

            if len(embeddings[0]) != settings.vector_size:
                raise RuntimeError(
                    f"Embedding dimension mismatch: expected {settings.vector_size}, got {len(embeddings[0])}"
                )

            if embedding_jsonl is not None:
                append_embedding_records(
                    embedding_jsonl,
                    records=records,
                    embeddings=embeddings,
                    collection_name=settings.collection_name,
                    embedding_model=settings.embedding_model,
                    vector_size=settings.vector_size,
                )
                embedding_record_count += len(records)
                embedding_record_ids.update(record["chunk_id"] for record in records)

            if settings.dry_run or not settings.upsert_to_qdrant:
                continue

            if client is None:
                raise RuntimeError("Qdrant client was not initialized.")
            points = [
                PointStruct(
                    id=record["chunk_id"],
                    vector=vector.tolist(),
                    payload=build_payload(record),
                )
                for record, vector in zip(records, embeddings)
            ]
            client.upsert(collection_name=settings.collection_name, points=points, wait=True)
            upserted += len(points)
            logging.info("Upserted %s point(s), total_upserted=%s", len(points), upserted)

        logging.info("silver_rows_seen=%s", total_rows)
        logging.info("valid_chunks=%s", valid_rows)
        logging.info("points_upserted=%s", upserted)
        if client is not None:
            collection_count = client.count(collection_name=settings.collection_name, exact=True).count
            logging.info("qdrant_collection=%s", settings.collection_name)
            logging.info("qdrant_collection_point_count=%s", collection_count)
        else:
            logging.info("qdrant_upsert_skipped=true")

        if embedding_jsonl is not None:
            if settings.merge_embedding_output:
                embedding_jsonl = merge_existing_embedding_output(
                    new_embedding_file=embedding_jsonl,
                    new_record_ids=embedding_record_ids,
                    gcs_path=settings.embedding_output_path,
                    work_dir=Path(temp_dir),
                )
            upload_file_to_gcs(embedding_jsonl, settings.embedding_output_path)
            logging.info("embedding_records_saved=%s", embedding_record_count)
            logging.info("embedding_output_path=%s", settings.embedding_output_path)

    return 0


def load_env_files() -> None:
    if load_dotenv is None:
        return
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")
    load_dotenv(project_root / "backend" / ".env", override=True)


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def require_value(value: str | None, name: str) -> str:
    if not value:
        raise RuntimeError(f"Missing required value: {name}")
    return value


def download_parquet_parts(gcs_path: str, download_dir: Path) -> list[Path]:
    try:
        return download_parquet_parts_with_gcloud(gcs_path, download_dir)
    except (subprocess.CalledProcessError, RuntimeError) as exc:
        logging.warning("gcloud storage download failed, falling back to Python GCS client: %s", exc)

    bucket_name, prefix = parse_gcs_path(gcs_path)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    parquet_files: list[Path] = []

    try:
        for blob in client.list_blobs(bucket, prefix=prefix):
            name = blob.name
            if not name.endswith(".parquet") or "/_temporary/" in name:
                continue
            destination = download_dir / Path(name).name
            logging.info("Downloading gs://%s/%s", bucket_name, name)
            blob.download_to_filename(destination)
            parquet_files.append(destination)
    except google_exceptions.GoogleAPICallError as exc:
        logging.warning("Python GCS client failed, falling back to gcloud storage: %s", exc)
        return download_parquet_parts_with_gcloud(gcs_path, download_dir)

    parquet_files.sort()
    logging.info("Downloaded %s Parquet part file(s)", len(parquet_files))
    return parquet_files


def download_parquet_parts_with_gcloud(gcs_path: str, download_dir: Path) -> list[Path]:
    gcloud = find_gcloud_executable()
    list_result = subprocess.run(
        [gcloud, "storage", "ls", f"{gcs_path.rstrip('/')}/*.parquet"],
        check=True,
        text=True,
        capture_output=True,
    )
    parquet_uris = [
        line.strip()
        for line in list_result.stdout.splitlines()
        if line.strip().endswith(".parquet") and "/_temporary/" not in line
    ]

    parquet_files: list[Path] = []
    for uri in parquet_uris:
        destination = download_dir / Path(uri).name
        logging.info("Downloading %s with gcloud storage", uri)
        subprocess.run([gcloud, "storage", "cp", uri, str(destination)], check=True)
        parquet_files.append(destination)

    parquet_files.sort()
    logging.info("Downloaded %s Parquet part file(s)", len(parquet_files))
    return parquet_files


def upload_file_to_gcs(local_file: Path, gcs_path: str) -> None:
    gcloud = find_gcloud_executable()
    try:
        logging.info("Uploading embedding artifact to %s with gcloud storage", gcs_path)
        subprocess.run([gcloud, "storage", "cp", str(local_file), gcs_path], check=True)
        return
    except subprocess.CalledProcessError as exc:
        logging.warning("gcloud storage upload failed, falling back to Python GCS client: %s", exc)

    bucket_name, blob_name = parse_gcs_file_path(gcs_path)
    client = storage.Client()
    try:
        logging.info("Uploading embedding artifact to %s", gcs_path)
        client.bucket(bucket_name).blob(blob_name).upload_from_filename(local_file)
    except google_exceptions.GoogleAPICallError as exc:
        raise RuntimeError(f"Failed to upload embedding artifact to {gcs_path}") from exc


def download_file_from_gcs(gcs_path: str, local_file: Path) -> bool:
    gcloud = find_gcloud_executable()
    result = subprocess.run([gcloud, "storage", "cp", gcs_path, str(local_file)], check=False)
    if result.returncode == 0 and local_file.exists():
        return True

    bucket_name, blob_name = parse_gcs_file_path(gcs_path)
    client = storage.Client()
    try:
        blob = client.bucket(bucket_name).blob(blob_name)
        if not blob.exists():
            return False
        logging.info("Downloading existing embedding artifact from %s", gcs_path)
        blob.download_to_filename(local_file)
        return True
    except google_exceptions.GoogleAPICallError as exc:
        logging.warning("Python GCS download failed after gcloud storage fallback: %s", exc)
        return False


def merge_existing_embedding_output(
    *,
    new_embedding_file: Path,
    new_record_ids: set[str],
    gcs_path: str,
    work_dir: Path,
) -> Path:
    if not new_record_ids:
        return new_embedding_file

    existing_file = work_dir / "existing_embeddings.jsonl"
    merged_file = work_dir / "merged_embeddings.jsonl"
    existing_found = download_file_from_gcs(gcs_path, existing_file)

    with merged_file.open("w", encoding="utf-8") as output:
        if existing_found:
            with existing_file.open("r", encoding="utf-8") as existing:
                for line in existing:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        logging.warning("Skipping invalid JSONL record while merging embeddings.")
                        continue
                    if record.get("id") in new_record_ids:
                        continue
                    output.write(json.dumps(record, ensure_ascii=False) + "\n")

        with new_embedding_file.open("r", encoding="utf-8") as new_records:
            written_new_ids: set[str] = set()
            for line in new_records:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logging.warning("Skipping invalid new JSONL record while merging embeddings.")
                    continue
                record_id = record.get("id")
                if record_id in written_new_ids:
                    continue
                if isinstance(record_id, str):
                    written_new_ids.add(record_id)
                output.write(json.dumps(record, ensure_ascii=False) + "\n")

    return merged_file


def parse_gcs_file_path(gcs_path: str) -> tuple[str, str]:
    parsed = urlparse(gcs_path)
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path.strip("/"):
        raise RuntimeError(f"Expected gs://bucket/path file path, got {gcs_path}")
    return parsed.netloc, parsed.path.lstrip("/")


def find_gcloud_executable() -> str:
    for candidate in ("gcloud", "gcloud.cmd"):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    raise RuntimeError("gcloud executable not found on PATH; cannot fallback to gcloud storage.")


def parse_gcs_path(gcs_path: str) -> tuple[str, str]:
    parsed = urlparse(gcs_path)
    if parsed.scheme != "gs" or not parsed.netloc:
        raise RuntimeError(f"Expected gs:// path, got {gcs_path}")
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return parsed.netloc, prefix


def append_embedding_records(
    output_file: Path,
    *,
    records: list[dict[str, Any]],
    embeddings,
    collection_name: str,
    embedding_model: str,
    vector_size: int,
) -> None:
    generated_at = datetime.now(UTC).isoformat()
    with output_file.open("a", encoding="utf-8") as handle:
        for record, vector in zip(records, embeddings):
            payload = build_payload(record)
            json_record = {
                "id": record["chunk_id"],
                "collection": collection_name,
                "embedding_model": embedding_model,
                "vector_size": vector_size,
                "vector": vector.tolist(),
                "payload": payload,
                "generated_at": generated_at,
            }
            handle.write(json.dumps(json_record, ensure_ascii=False) + "\n")


def iter_valid_batches(
    parquet_files: list[Path],
    batch_size: int,
    *,
    source_path: str | None = None,
    source_prefix: str | None = None,
    source_file: str | None = None,
):
    for parquet_file in parquet_files:
        parquet = pq.ParquetFile(parquet_file)
        for record_batch in parquet.iter_batches(batch_size=batch_size):
            columns = record_batch.to_pydict()
            total_rows = record_batch.num_rows
            records = []
            for index in range(total_rows):
                record = {column: values[index] for column, values in columns.items()}
                if is_valid_record(
                    record,
                    source_path=source_path,
                    source_prefix=source_prefix,
                    source_file=source_file,
                ):
                    records.append(record)
            yield {"total_rows": total_rows, "records": records}


def is_valid_record(
    record: dict[str, Any],
    *,
    source_path: str | None = None,
    source_prefix: str | None = None,
    source_file: str | None = None,
) -> bool:
    text = record.get("chunk_text")
    chunk_id = record.get("chunk_id")
    record_source_path = record.get("source_path")
    return (
        record.get("status") == "ok"
        and isinstance(text, str)
        and bool(text.strip())
        and isinstance(chunk_id, str)
        and bool(chunk_id.strip())
        and (source_path is None or record_source_path == source_path)
        and (
            source_prefix is None
            or (isinstance(record_source_path, str) and record_source_path.startswith(source_prefix))
        )
        and (source_file is None or record.get("source_file") == source_file)
    )


def build_payload(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "chunk_text": record.get("chunk_text"),
        "document_id": record.get("document_id"),
        "source_path": record.get("source_path"),
        "source_file": record.get("source_file"),
        "page": record.get("page_number"),
        "chunk_index": record.get("chunk_index"),
        "language": record.get("language"),
        "processed_at": stringify(record.get("processed_at")),
        "chunk_size": record.get("chunk_size"),
        "chunk_overlap": record.get("chunk_overlap"),
    }


def stringify(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def ensure_collection(client: QdrantClient, collection_name: str, vector_size: int) -> None:
    if not client.collection_exists(collection_name):
        logging.info("Creating Qdrant collection %s", collection_name)
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        )
        return

    info = client.get_collection(collection_name)
    actual_size, actual_distance = read_vector_config(info)
    if actual_size != vector_size or str(actual_distance).lower().split(".")[-1] != "cosine":
        raise RuntimeError(
            "Qdrant collection config mismatch: "
            f"collection={collection_name}, expected size={vector_size} distance=Cosine, "
            f"actual size={actual_size} distance={actual_distance}. "
            "Stop here; do not delete/recreate the collection without explicit approval."
        )
    logging.info("Qdrant collection config OK: %s size=%s distance=%s", collection_name, actual_size, actual_distance)


def read_vector_config(collection_info) -> tuple[int | None, Any]:
    vectors = collection_info.config.params.vectors
    if isinstance(vectors, dict):
        first_vector = next(iter(vectors.values()))
        return getattr(first_vector, "size", None), getattr(first_vector, "distance", None)
    return getattr(vectors, "size", None), getattr(vectors, "distance", None)


if __name__ == "__main__":
    raise SystemExit(main())
