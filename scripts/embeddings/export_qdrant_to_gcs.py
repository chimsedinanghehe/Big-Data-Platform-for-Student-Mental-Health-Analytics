from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import logging
import os
from pathlib import Path
import tempfile
from typing import Any
from urllib.parse import urlparse

from dotenv import load_dotenv
from google.cloud import storage
from qdrant_client import QdrantClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BATCH_SIZE = 256
DEFAULT_PREFIX = "vector_backup/qdrant"


@dataclass(frozen=True)
class ExportConfig:
    qdrant_host: str
    qdrant_port: int
    qdrant_api_key: str | None
    collection_name: str
    gcs_bucket_name: str
    batch_size: int
    output_format: str
    output_dir: Path
    gcs_prefix: str
    filename_prefix: str
    prefer_https: bool


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    configure_logging()

    try:
        config = parse_args()
        local_file, object_name, total_points = export_collection(config)
        gcs_uri = upload_to_gcs(
            bucket_name=config.gcs_bucket_name,
            object_name=object_name,
            local_file=local_file,
        )
    except Exception:
        logging.exception("Qdrant export failed.")
        return 1

    logging.info("Export completed: %s points uploaded to %s", total_points, gcs_uri)
    print(gcs_uri)
    return 0


def parse_args() -> ExportConfig:
    parser = argparse.ArgumentParser(
        description="Export all Qdrant points to JSONL or Parquet and upload to Google Cloud Storage.",
    )
    parser.add_argument("--qdrant-host", default=os.getenv("QDRANT_HOST"))
    parser.add_argument("--qdrant-port", type=int, default=_env_int("QDRANT_PORT", None))
    parser.add_argument("--qdrant-api-key", default=os.getenv("QDRANT_API_KEY"))
    parser.add_argument("--collection-name", default=os.getenv("QDRANT_COLLECTION"))
    parser.add_argument("--gcs-bucket-name", default=os.getenv("GCS_BUCKET_NAME") or os.getenv("GCS_BUCKET"))
    parser.add_argument("--batch-size", type=int, default=_env_int("QDRANT_EXPORT_BATCH_SIZE", DEFAULT_BATCH_SIZE))
    parser.add_argument("--format", choices=("jsonl", "parquet"), default=os.getenv("QDRANT_EXPORT_FORMAT", "jsonl"))
    parser.add_argument("--output-dir", type=Path, default=Path(os.getenv("QDRANT_EXPORT_OUTPUT_DIR", tempfile.gettempdir())))
    parser.add_argument("--gcs-prefix", default=os.getenv("QDRANT_EXPORT_GCS_PREFIX", DEFAULT_PREFIX))
    parser.add_argument("--filename-prefix", default=os.getenv("QDRANT_EXPORT_FILENAME_PREFIX", "export"))
    parser.add_argument("--https", action="store_true", default=_env_bool("QDRANT_HTTPS", False))
    args = parser.parse_args()

    host = args.qdrant_host
    port = args.qdrant_port
    prefer_https = args.https

    if not host or port is None:
        parsed_host, parsed_port, parsed_https = _parse_qdrant_url(os.getenv("QDRANT_URL"))
        host = host or parsed_host
        port = port if port is not None else parsed_port
        prefer_https = prefer_https or parsed_https

    missing = []
    if not host:
        missing.append("QDRANT_HOST or QDRANT_URL")
    if port is None:
        missing.append("QDRANT_PORT or QDRANT_URL with port")
    if not args.collection_name:
        missing.append("QDRANT_COLLECTION or --collection-name")
    if not args.gcs_bucket_name:
        missing.append("GCS_BUCKET_NAME/GCS_BUCKET or --gcs-bucket-name")
    if missing:
        raise RuntimeError(f"Missing required config: {', '.join(missing)}")
    if args.batch_size <= 0:
        raise RuntimeError("--batch-size must be greater than 0.")

    return ExportConfig(
        qdrant_host=str(host),
        qdrant_port=int(port),
        qdrant_api_key=args.qdrant_api_key,
        collection_name=args.collection_name,
        gcs_bucket_name=args.gcs_bucket_name,
        batch_size=args.batch_size,
        output_format=args.format,
        output_dir=args.output_dir,
        gcs_prefix=args.gcs_prefix.strip("/"),
        filename_prefix=args.filename_prefix.strip("_"),
        prefer_https=prefer_https,
    )


def export_collection(config: ExportConfig) -> tuple[Path, str, int]:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    extension = config.output_format
    file_name = f"{config.filename_prefix}_{timestamp}.{extension}"
    local_file = config.output_dir / f"qdrant_{config.collection_name}_{file_name}"
    object_name = f"{config.gcs_prefix}/{config.collection_name}/{file_name}"

    logging.info(
        "Connecting to Qdrant host=%s port=%s collection=%s",
        config.qdrant_host,
        config.qdrant_port,
        config.collection_name,
    )
    client = QdrantClient(
        host=config.qdrant_host,
        port=config.qdrant_port,
        api_key=config.qdrant_api_key,
        https=config.prefer_https,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    if config.output_format == "jsonl":
        total_points = export_jsonl(client, config, local_file)
    else:
        total_points = export_parquet(client, config, local_file)

    return local_file, object_name, total_points


def export_jsonl(client: QdrantClient, config: ExportConfig, local_file: Path) -> int:
    total_points = 0
    next_offset: Any = None

    logging.info("Writing JSONL export to %s", local_file)
    with local_file.open("w", encoding="utf-8") as writer:
        while True:
            points, next_offset = client.scroll(
                collection_name=config.collection_name,
                limit=config.batch_size,
                offset=next_offset,
                with_payload=True,
                with_vectors=True,
            )

            for point in points:
                writer.write(json.dumps(point_to_record(point), ensure_ascii=False, default=str) + "\n")

            total_points += len(points)
            logging.info("Exported %s points so far.", total_points)

            if next_offset is None:
                break

    return total_points


def export_parquet(client: QdrantClient, config: ExportConfig, local_file: Path) -> int:
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("Parquet export requires pandas and pyarrow. Install with: pip install pandas pyarrow") from exc

    records: list[dict[str, Any]] = []
    total_points = 0
    next_offset: Any = None

    logging.info("Collecting records for Parquet export to %s", local_file)
    while True:
        points, next_offset = client.scroll(
            collection_name=config.collection_name,
            limit=config.batch_size,
            offset=next_offset,
            with_payload=True,
            with_vectors=True,
        )
        records.extend(point_to_record(point) for point in points)
        total_points += len(points)
        logging.info("Loaded %s points so far.", total_points)

        if next_offset is None:
            break

    dataframe = pd.DataFrame.from_records(records)
    for column in ("id", "vector", "payload"):
        if column in dataframe.columns:
            dataframe[column] = dataframe[column].map(lambda value: json.dumps(value, ensure_ascii=False, default=str))
    dataframe.to_parquet(local_file, index=False)
    return total_points


def point_to_record(point: Any) -> dict[str, Any]:
    payload = point.payload or {}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

    return {
        "id": point.id,
        "vector": point.vector,
        "payload": payload,
        "chunk_text": first_present(payload, ("chunk_text", "text", "page_content", "content")),
        "source": first_present(payload, ("source",), metadata),
        "document_id": first_present(payload, ("document_id", "doc_id"), metadata),
        "page": first_present(payload, ("page", "page_number"), metadata),
        "created_at": first_present(payload, ("created_at", "createdAt", "timestamp"), metadata),
    }


def first_present(payload: dict[str, Any], keys: tuple[str, ...], metadata: dict[str, Any] | None = None) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    if metadata:
        for key in keys:
            if key in metadata and metadata[key] is not None:
                return metadata[key]
    return None


def upload_to_gcs(bucket_name: str, object_name: str, local_file: Path) -> str:
    logging.info("Uploading %s to gs://%s/%s", local_file, bucket_name, object_name)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    content_type = "application/jsonl" if local_file.suffix == ".jsonl" else "application/octet-stream"
    blob.upload_from_filename(str(local_file), content_type=content_type)
    return f"gs://{bucket_name}/{object_name}"


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def _parse_qdrant_url(raw_url: str | None) -> tuple[str | None, int | None, bool]:
    if not raw_url:
        return None, None, False
    parsed = urlparse(raw_url)
    if not parsed.hostname:
        return None, None, False
    return parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 6333), parsed.scheme == "https"


def _env_int(name: str, default: int | None) -> int | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer.") from exc


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} must be true or false.")


if __name__ == "__main__":
    raise SystemExit(main())
