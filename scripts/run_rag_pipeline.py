from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


DEFAULT_PROJECT = "student-mental-health-496205"
DEFAULT_BUCKET = "student-mental-health-lake-nhom1-2026"
DEFAULT_COLLECTION = "student_mental_health_v1"
DEFAULT_CHUNK_SIZE = 500
DEFAULT_CHUNK_OVERLAP = 100


ROOT_DIR = Path(__file__).resolve().parents[1]
PREPROCESS_SCRIPT = ROOT_DIR / "scripts" / "preprocessing" / "run_rag_preprocessing_dataproc.py"
INDEX_SCRIPT = ROOT_DIR / "scripts" / "embeddings" / "index_gold_rag_chunks_to_qdrant.py"
EXPORT_SCRIPT = ROOT_DIR / "scripts" / "embeddings" / "export_qdrant_to_gcs.py"


def main() -> int:
    config = load_config()
    while True:
        print_header(config)
        print("1. Run incremental pipeline for exact Bronze GCS file path(s)")
        print("2. Run incremental pipeline for a Bronze GCS prefix/folder")
        print("3. Full rebuild Silver/Gold from all Bronze documents")
        print("4. Save embedding artifact from all Gold chunks")
        print("5. Upsert all Gold chunks to Qdrant")
        print("6. Full rebuild end-to-end")
        print("7. Export Qdrant collection backup to GCS")
        print("8. Change config for this run")
        print("0. Exit")
        choice = input("\nChoose: ").strip()

        if choice == "1":
            incremental_from_gcs_paths(config)
        elif choice == "2":
            incremental_from_gcs_prefix(config)
        elif choice == "3":
            full_preprocess(config)
        elif choice == "4":
            save_all_embeddings(config)
        elif choice == "5":
            upsert_all_to_qdrant(config)
        elif choice == "6":
            full_rebuild_end_to_end(config)
        elif choice == "7":
            export_qdrant_backup(config)
        elif choice == "8":
            config = edit_config(config)
        elif choice == "0":
            return 0
        else:
            print("Invalid choice.")

        input("\nPress Enter to continue...")


def load_config() -> dict[str, str | int]:
    return {
        "project": os.getenv("RAG_PROJECT", DEFAULT_PROJECT),
        "bucket": os.getenv("RAG_BUCKET", DEFAULT_BUCKET),
        "collection": os.getenv("RAG_COLLECTION", DEFAULT_COLLECTION),
        "chunk_size": int(os.getenv("RAG_CHUNK_SIZE", str(DEFAULT_CHUNK_SIZE))),
        "chunk_overlap": int(os.getenv("RAG_CHUNK_OVERLAP", str(DEFAULT_CHUNK_OVERLAP))),
    }


def print_header(config: dict[str, str | int]) -> None:
    print("\n" + "=" * 72)
    print("RAG Pipeline Runner")
    print("=" * 72)
    print(f"project:     {config['project']}")
    print(f"bucket:      gs://{config['bucket']}")
    print(f"collection:  {config['collection']}")
    print(f"chunks:      size={config['chunk_size']} overlap={config['chunk_overlap']}")
    print("-" * 72)


def edit_config(config: dict[str, str | int]) -> dict[str, str | int]:
    updated = dict(config)
    updated["project"] = prompt("Project", str(config["project"]))
    updated["bucket"] = prompt("Bucket name", str(config["bucket"])).removeprefix("gs://").strip("/")
    updated["collection"] = prompt("Qdrant collection", str(config["collection"]))
    updated["chunk_size"] = prompt_int("Chunk size", int(config["chunk_size"]))
    updated["chunk_overlap"] = prompt_int("Chunk overlap", int(config["chunk_overlap"]))
    return updated


def incremental_from_gcs_paths(config: dict[str, str | int]) -> None:
    raw_paths = prompt_required("Exact GCS source path(s). Separate multiple entries with ';'")
    source_paths = split_multi_value(raw_paths)
    ensure_gcs_paths(source_paths)
    run_incremental_paths(config, source_paths)


def incremental_from_gcs_prefix(config: dict[str, str | int]) -> None:
    default_prefix = f"gs://{config['bucket']}/bronze/knowledge_base/"
    source_prefix = normalize_gcs_prefix(prompt("Bronze GCS prefix/folder", default_prefix))
    if source_prefix == default_prefix and not yes_no(
        "This is the whole Bronze folder. Append mode may duplicate old rows. Continue?",
        default=False,
    ):
        print("Cancelled.")
        return
    preprocess(config, input_path=source_prefix, output_mode="append")
    index(config, source_prefix=source_prefix, merge_embedding_output=True, upsert_to_qdrant=False)
    if yes_no("Upsert chunks from this prefix to Qdrant now?", default=True):
        index(config, source_prefix=source_prefix, no_save_embeddings=True, upsert_to_qdrant=True)


def run_incremental_paths(config: dict[str, str | int], source_paths: list[str]) -> None:
    if not source_paths:
        raise ValueError("No source paths provided.")

    preprocess(
        config,
        input_path=";".join(source_paths),
        output_mode="append",
    )

    for source_path in source_paths:
        index(
            config,
            source_path=source_path,
            merge_embedding_output=True,
            upsert_to_qdrant=False,
        )

    if yes_no(f"Upsert {len(source_paths)} source(s) to Qdrant now?", default=True):
        for source_path in source_paths:
            index(
                config,
                source_path=source_path,
                no_save_embeddings=True,
                upsert_to_qdrant=True,
            )


def split_multi_value(raw_value: str) -> list[str]:
    values = [strip_quotes(value) for value in raw_value.replace("\n", ";").split(";")]
    return [value for value in values if value]


def ensure_gcs_paths(source_paths: list[str]) -> None:
    invalid_paths = [source_path for source_path in source_paths if not source_path.startswith("gs://")]
    if invalid_paths:
        raise ValueError("All source paths must be GCS paths: " + ", ".join(invalid_paths))


def normalize_gcs_prefix(source_prefix: str) -> str:
    source_prefix = strip_quotes(source_prefix).strip()
    if not source_prefix.startswith("gs://"):
        raise ValueError(f"Source prefix must be a GCS path: {source_prefix}")
    return source_prefix.rstrip("/") + "/"


def full_preprocess(config: dict[str, str | int]) -> None:
    if not yes_no("Overwrite Silver/Gold for the whole knowledge base?", default=False):
        print("Cancelled.")
        return
    preprocess(
        config,
        input_path=f"gs://{config['bucket']}/bronze/knowledge_base/",
        output_mode="overwrite",
    )


def save_all_embeddings(config: dict[str, str | int]) -> None:
    index(config, upsert_to_qdrant=False)


def upsert_all_to_qdrant(config: dict[str, str | int]) -> None:
    if not yes_no(f"Upsert all Gold chunks to Qdrant collection {config['collection']}?", default=False):
        print("Cancelled.")
        return
    index(config, no_save_embeddings=True, upsert_to_qdrant=True)


def full_rebuild_end_to_end(config: dict[str, str | int]) -> None:
    if not yes_no("Run full overwrite preprocessing, regenerate embeddings, then optionally upsert?", default=False):
        print("Cancelled.")
        return
    preprocess(
        config,
        input_path=f"gs://{config['bucket']}/bronze/knowledge_base/",
        output_mode="overwrite",
    )
    index(config, upsert_to_qdrant=False)
    if yes_no("Upsert rebuilt Gold chunks to Qdrant now?", default=False):
        index(config, no_save_embeddings=True, upsert_to_qdrant=True)


def export_qdrant_backup(config: dict[str, str | int]) -> None:
    command = [
        sys.executable,
        str(EXPORT_SCRIPT),
        f"--gcs-bucket-name={config['bucket']}",
        f"--collection-name={config['collection']}",
        "--gcs-prefix=vector_backup/qdrant",
        "--filename-prefix=export_before_rebuild",
    ]
    run(command)


def preprocess(config: dict[str, str | int], *, input_path: str, output_mode: str) -> None:
    command = [
        sys.executable,
        str(PREPROCESS_SCRIPT),
        f"--project={config['project']}",
        f"--bucket={config['bucket']}",
        f"--input_path={input_path}",
        f"--output-mode={output_mode}",
        f"--chunk-size={config['chunk_size']}",
        f"--chunk-overlap={config['chunk_overlap']}",
    ]
    run(command)


def index(
    config: dict[str, str | int],
    *,
    source_path: str | None = None,
    source_prefix: str | None = None,
    merge_embedding_output: bool = False,
    no_save_embeddings: bool = False,
    upsert_to_qdrant: bool = False,
) -> None:
    command = [
        sys.executable,
        str(INDEX_SCRIPT),
        f"--bucket={config['bucket']}",
        f"--collection-name={config['collection']}",
    ]
    if source_path:
        command.append(f"--source-path={source_path}")
    if source_prefix:
        command.append(f"--source-prefix={source_prefix}")
    if merge_embedding_output:
        command.append("--merge-embedding-output")
    if no_save_embeddings:
        command.append("--no-save-embeddings")
    if upsert_to_qdrant:
        command.append("--upsert-to-qdrant")
    run(command)


def run(command: list[str]) -> None:
    print("\n+ " + " ".join(command))
    subprocess.run(command, cwd=ROOT_DIR, check=True)


def prompt(label: str, default: str) -> str:
    value = input(f"{label} [{default}]: ").strip()
    return value or default


def prompt_required(label: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default:
            return default
        print("Value is required.")


def prompt_int(label: str, default: int) -> int:
    while True:
        value = prompt(label, str(default))
        try:
            return int(value)
        except ValueError:
            print("Enter a valid integer.")


def yes_no(question: str, *, default: bool) -> bool:
    suffix = "Y/n" if default else "y/N"
    while True:
        value = input(f"{question} [{suffix}]: ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        print("Enter y or n.")


def strip_quotes(value: str) -> str:
    return value.strip().strip('"').strip("'")


if __name__ == "__main__":
    raise SystemExit(main())
