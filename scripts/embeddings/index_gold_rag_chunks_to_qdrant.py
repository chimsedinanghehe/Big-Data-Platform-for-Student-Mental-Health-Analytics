from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import subprocess
import sys


DEFAULT_BUCKET = "student-mental-health-lake-nhom1-2026"
DEFAULT_COLLECTION = "student_mental_health_v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate embeddings for Gold RAG chunks, save them to GCS, and optionally upsert to Qdrant."
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument(
        "--version",
        default=None,
        help="Optional version partition. Omit it to read gold/rag_chunks/parquet/ and write vector/embeddings/<collection>/embeddings.jsonl.",
    )
    parser.add_argument("--collection-name", default=DEFAULT_COLLECTION)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--upsert-to-qdrant", action="store_true", help="Write generated vectors to Qdrant. Default is embedding artifact only.")
    parser.add_argument("--input_path", default=None)
    parser.add_argument("--embedding_output_path", default=None)
    parser.add_argument(
        "--embedding-artifact-mode",
        choices=("consolidated", "run"),
        default="consolidated",
        help="consolidated writes vector/embeddings/<collection>/embeddings.jsonl; run writes an append-only run artifact.",
    )
    parser.add_argument("--run-id", default=None, help="Optional run id for append-only embedding artifacts.")
    parser.add_argument("--source-path", default=None, help="Only process chunks from this exact source_path.")
    parser.add_argument("--source-prefix", default=None, help="Only process chunks whose source_path starts with this prefix.")
    parser.add_argument("--source-file", default=None, help="Only process chunks from this exact source_file.")
    parser.add_argument("--merge-embedding-output", action="store_true", help="Merge into existing embeddings.jsonl by id instead of overwriting it.")
    parser.add_argument("--no-save-embeddings", action="store_true", help="Do not write embedding artifact to GCS.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    input_path = args.input_path or f"{versioned_path(f'gs://{args.bucket}/gold/rag_chunks', args.version)}/parquet/"
    run_id = args.run_id or f"run={datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    embedding_base_path = versioned_path(f"gs://{args.bucket}/vector/embeddings/{args.collection_name}", args.version)
    if args.no_save_embeddings:
        embedding_output_path = None
        manifest_output_path = None
    elif args.embedding_output_path:
        embedding_output_path = args.embedding_output_path
        manifest_output_path = None
    elif args.embedding_artifact_mode == "run":
        embedding_output_path = f"{embedding_base_path}/incremental/{run_id}/embeddings.jsonl"
        manifest_output_path = f"{embedding_base_path}/incremental/{run_id}/manifest.json"
    else:
        embedding_output_path = f"{embedding_base_path}/embeddings.jsonl"
        manifest_output_path = f"{embedding_base_path}/manifest.json"

    command = [
        sys.executable,
        str(script_dir / "silver_to_qdrant.py"),
        f"--input_path={input_path}",
        f"--collection-name={args.collection_name}",
        f"--batch-size={args.batch_size}",
    ]
    if embedding_output_path:
        command.append(f"--embedding-output-path={embedding_output_path}")
    if manifest_output_path:
        command.append(f"--manifest-output-path={manifest_output_path}")
    if args.source_path:
        command.append(f"--source-path={args.source_path}")
    if args.source_prefix:
        command.append(f"--source-prefix={args.source_prefix}")
    if args.source_file:
        command.append(f"--source-file={args.source_file}")
    if args.merge_embedding_output:
        command.append("--merge-embedding-output")
    if args.no_save_embeddings:
        command.append("--no-save-embeddings")
    if args.dry_run:
        command.append("--dry-run")
    if not args.upsert_to_qdrant:
        command.append("--skip-qdrant-upsert")

    print(f"input_path={input_path}")
    if embedding_output_path:
        print(f"embedding_output_path={embedding_output_path}")
    else:
        print("embedding_output_path=(skipped)")
    if manifest_output_path:
        print(f"manifest_output_path={manifest_output_path}")
    print(f"embedding_artifact_mode={args.embedding_artifact_mode}")
    print(f"upsert_to_qdrant={str(args.upsert_to_qdrant).lower()}")
    print(f"started_at={datetime.now(UTC).isoformat()}")
    print("+ " + " ".join(command))
    subprocess.run(command, check=True)
    return 0


def versioned_path(base_path: str, version: str | None) -> str:
    if not version:
        return base_path
    return f"{base_path.rstrip('/')}/{version.strip('/')}"


if __name__ == "__main__":
    raise SystemExit(main())
