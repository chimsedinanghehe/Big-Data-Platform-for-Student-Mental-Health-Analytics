from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap


DEFAULT_PROJECT = "student-mental-health-496205"
DEFAULT_BUCKET = "student-mental-health-lake-nhom1-2026"
DEFAULT_CLUSTER = "pdf-clean-cluster"
DEFAULT_REGION = "asia-southeast1"
DEFAULT_ZONE = "asia-southeast1-a"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the RAG preprocessing flow on Dataproc: Bronze knowledge base -> Silver clean docs -> Gold RAG chunks."
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--input_path", default=None, help="Defaults to gs://<bucket>/bronze/knowledge_base/")
    parser.add_argument(
        "--version",
        default=None,
        help="Optional version partition. Omit it to write directly to silver/knowledge_base_clean/ and gold/rag_chunks/.",
    )
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--zone", default=DEFAULT_ZONE)
    parser.add_argument("--machine-type", default="e2-standard-4")
    parser.add_argument("--image-version", default="2.2-debian12")
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--chunk-overlap", type=int, default=100)
    parser.add_argument("--output-mode", choices=("errorifexists", "overwrite", "append"), default="overwrite")
    parser.add_argument("--dependency-zip", default=None, help="Defaults to gs://<bucket>/jobs/deps/pypdf_deps.zip")
    parser.add_argument("--skip-cluster-create", action="store_true")
    parser.add_argument("--keep-cluster", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    pdf_to_silver_path = script_dir / "pdf_to_silver.py"
    run_id = args.version or f"run={datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    input_path = args.input_path or f"gs://{args.bucket}/bronze/knowledge_base/"
    clean_output = versioned_path(f"gs://{args.bucket}/silver/knowledge_base_clean", args.version)
    chunk_output = versioned_path(f"gs://{args.bucket}/gold/rag_chunks", args.version)
    dependency_zip = args.dependency_zip or f"gs://{args.bucket}/jobs/deps/pypdf_deps.zip"

    print(f"project={args.project}")
    print(f"input_path={input_path}")
    print(f"clean_output={clean_output}")
    print(f"chunk_output={chunk_output}")
    print(f"dependency_zip={dependency_zip}")

    gcloud = find_gcloud_executable()
    run([gcloud, "storage", "cp", str(pdf_to_silver_path), f"gs://{args.bucket}/scripts/pdf_to_silver.py"])
    init_action_uri = upload_init_action(args, dependency_zip=dependency_zip, run_id=run_id)

    if not args.skip_cluster_create:
        run(
            [
                gcloud,
                "dataproc",
                "clusters",
                "create",
                args.cluster,
                f"--project={args.project}",
                f"--region={args.region}",
                f"--zone={args.zone}",
                "--single-node",
                f"--master-machine-type={args.machine_type}",
                f"--image-version={args.image_version}",
                f"--initialization-actions={init_action_uri}",
            ]
        )

    try:
        with tempfile.TemporaryDirectory(prefix="rag_preprocess_driver_") as temp_dir:
            clean_driver = Path(temp_dir) / "preprocess_clean_docs.py"
            chunk_driver = Path(temp_dir) / "preprocess_gold_chunks.py"
            write_driver(
                clean_driver,
                input_path=input_path,
                output_path=clean_output,
                output_mode=args.output_mode,
                emit_chunks=False,
                chunk_size=args.chunk_size,
                chunk_overlap=args.chunk_overlap,
            )
            write_driver(
                chunk_driver,
                input_path=input_path,
                output_path=chunk_output,
                output_mode=args.output_mode,
                emit_chunks=True,
                chunk_size=args.chunk_size,
                chunk_overlap=args.chunk_overlap,
            )

            clean_uri = f"gs://{args.bucket}/scripts/generated/{run_id}/preprocess_clean_docs.py"
            chunk_uri = f"gs://{args.bucket}/scripts/generated/{run_id}/preprocess_gold_chunks.py"
            run([gcloud, "storage", "cp", str(clean_driver), clean_uri])
            run([gcloud, "storage", "cp", str(chunk_driver), chunk_uri])

            submit_pyspark(args, clean_uri)
            submit_pyspark(args, chunk_uri)

        print("preprocessing_done=true")
        print(f"silver_knowledge_base_clean={clean_output}")
        print(f"gold_rag_chunks={chunk_output}")
        print(f"gold_rag_chunks_parquet={chunk_output}/parquet/")
        print(f"gold_rag_chunks_jsonl={chunk_output}/jsonl/")
        return 0
    finally:
        if not args.keep_cluster:
            run(
                [
                    find_gcloud_executable(),
                    "dataproc",
                    "clusters",
                    "delete",
                    args.cluster,
                    f"--project={args.project}",
                    f"--region={args.region}",
                    "--quiet",
                ],
                check=False,
            )


def upload_init_action(args: argparse.Namespace, *, dependency_zip: str, run_id: str) -> str:
    gcloud = find_gcloud_executable()
    with tempfile.TemporaryDirectory(prefix="rag_preprocess_init_") as temp_dir:
        init_script = Path(temp_dir) / "install_pypdf.sh"
        init_script.write_bytes(
            textwrap.dedent(
                f"""\
                #!/bin/bash
                set -euxo pipefail

                PYTHON_BIN="/opt/conda/default/bin/python"
                SITE_PACKAGES="$(${{PYTHON_BIN}} - <<'PY'
                import site
                print(site.getsitepackages()[0])
                PY
                )"

                gsutil cp "{dependency_zip}" /tmp/pypdf_deps.zip
                sudo unzip -o /tmp/pypdf_deps.zip -d "${{SITE_PACKAGES}}" || true
                "${{PYTHON_BIN}}" -c "import pypdf; print(pypdf.__version__)"
                """
            ).encode("utf-8"),
        )
        init_action_uri = f"gs://{args.bucket}/scripts/generated/{run_id}/install_pypdf.sh"
        run([gcloud, "storage", "cp", str(init_script), init_action_uri])
        return init_action_uri


def write_driver(
    path: Path,
    *,
    input_path: str,
    output_path: str,
    output_mode: str,
    emit_chunks: bool,
    chunk_size: int,
    chunk_overlap: int,
) -> None:
    driver_args = [
        "pdf_to_silver.py",
        f"--input_path={input_path}",
        f"--output_path={output_path}",
        f"--output_mode={output_mode}",
        "--output_format=both",
    ]
    if emit_chunks:
        driver_args.extend(
            [
                "--emit_chunks",
                f"--chunk_size={chunk_size}",
                f"--chunk_overlap={chunk_overlap}",
            ]
        )

    path.write_text(
        textwrap.dedent(
            f"""
            from __future__ import annotations

            import sys

            from pdf_to_silver import main


            if __name__ == "__main__":
                sys.argv = {driver_args!r}
                main()
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )


def submit_pyspark(args: argparse.Namespace, driver_uri: str) -> None:
    gcloud = find_gcloud_executable()
    run(
        [
            gcloud,
            "dataproc",
            "jobs",
            "submit",
            "pyspark",
            driver_uri,
            f"--project={args.project}",
            f"--cluster={args.cluster}",
            f"--region={args.region}",
            "--py-files",
            f"gs://{args.bucket}/scripts/pdf_to_silver.py",
        ]
    )


def run(command: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print("+ " + " ".join(command))
    return subprocess.run(command, check=check)


def find_gcloud_executable() -> str:
    for candidate in ("gcloud", "gcloud.cmd"):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    raise RuntimeError("gcloud executable not found on PATH.")


def versioned_path(base_path: str, version: str | None) -> str:
    if not version:
        return base_path
    return f"{base_path.rstrip('/')}/{version.strip('/')}"


if __name__ == "__main__":
    raise SystemExit(main())
