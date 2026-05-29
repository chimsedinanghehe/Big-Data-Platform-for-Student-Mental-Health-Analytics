from pathlib import Path
import os
import shutil
import subprocess
from tempfile import TemporaryDirectory

from langchain_community.document_loaders import DirectoryLoader, PyPDFLoader


def load_pdf_documents(path: str):
    loader = DirectoryLoader(
        path,
        glob="*.pdf",
        loader_cls=PyPDFLoader,
    )

    documents = loader.load()
    print(f"Loaded {len(documents)} PDF pages")

    return documents


def load_gcs_pdf_documents(bucket_name: str, prefix: str, skip_sources: set[str] | None = None):
    normalized_prefix = prefix.strip("/")
    source_uri = f"gs://{bucket_name}/{normalized_prefix}"
    skip_sources = skip_sources or set()

    result = _run_gcloud(["storage", "ls", f"{source_uri}/"])
    pdf_uris = [line.strip() for line in result.stdout.splitlines() if line.strip().lower().endswith(".pdf")]

    if not pdf_uris:
        raise RuntimeError(f"No PDF files found in {source_uri}/")

    new_pdf_uris = [pdf_uri for pdf_uri in pdf_uris if pdf_uri not in skip_sources]
    skipped_count = len(pdf_uris) - len(new_pdf_uris)

    if skipped_count:
        print(f"Skipping {skipped_count} already indexed PDF file(s)")

    if not new_pdf_uris:
        print(f"No new PDF files found in {source_uri}/")
        return []

    with TemporaryDirectory(prefix="rag_gcs_docs_") as temp_dir:
        temp_path = Path(temp_dir)
        for pdf_uri in new_pdf_uris:
            _run_gcloud(["storage", "cp", pdf_uri, str(temp_path)])

        documents = load_pdf_documents(str(temp_path))

    for document in documents:
        source = Path(document.metadata.get("source", "")).name
        document.metadata["source"] = f"{source_uri}/{source}"
        document.metadata["data_layer"] = "bronze"

    print(f"Loaded {len(documents)} PDF pages from {source_uri}/")
    return documents


def _run_gcloud(args: list[str]) -> subprocess.CompletedProcess[str]:
    gcloud_command = _gcloud_command()
    return subprocess.run(
        [*gcloud_command, *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _gcloud_command() -> list[str]:
    configured = os.getenv("GCLOUD_COMMAND")
    if configured:
        return [configured]

    for name in ("gcloud.cmd", "gcloud.exe", "gcloud"):
        found = shutil.which(name)
        if found:
            return [found]

    powershell_gcloud = Path.home() / "AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud.ps1"
    if powershell_gcloud.exists():
        return [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(powershell_gcloud),
        ]

    raise RuntimeError("gcloud command was not found. Install Google Cloud SDK or set GCLOUD_COMMAND.")
