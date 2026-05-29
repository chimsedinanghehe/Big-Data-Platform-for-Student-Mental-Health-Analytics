from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
from io import BytesIO
import re
from pathlib import PurePosixPath

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


SEPARATORS = ("\n\n", "\n", ".", " ")
SUPPORTED_EXTENSIONS = {".pdf", ".txt", ".md", ".html", ".htm", ".csv"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preprocess bronze knowledge base files into silver RAG chunks.")
    parser.add_argument("--input", required=True, help="GCS input prefix, for example gs://bucket/bronze/knowledge_base/")
    parser.add_argument(
        "--output",
        required=True,
        help="GCS output prefix, for example gs://bucket/silver/knowledge_base_cleaned/",
    )
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--chunk-overlap", type=int, default=100)
    parser.add_argument("--format", choices=("json", "parquet"), default="json")
    parser.add_argument("--output-mode", choices=("overwrite", "append"), default="overwrite")
    parser.add_argument("--recursive", action="store_true", default=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("rag-knowledgebase-preprocess").getOrCreate()

    input_path = args.input.rstrip("/")
    output_path = args.output.rstrip("/")
    created_at = datetime.now(UTC).isoformat()

    files = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", str(args.recursive).lower())
        .load(input_path)
        .select("path", "content", "length", "modificationTime")
    )

    chunks = files.rdd.mapPartitions(
        lambda rows: extract_partition_chunks(
            rows=rows,
            chunk_size=args.chunk_size,
            chunk_overlap=args.chunk_overlap,
            created_at=created_at,
        )
    )

    output_df = spark.createDataFrame(chunks, schema=output_schema())
    writer = output_df.write.mode(args.output_mode)
    if args.format == "parquet":
        writer.parquet(output_path)
    else:
        writer.json(output_path)

    spark.stop()


def extract_partition_chunks(rows, chunk_size: int, chunk_overlap: int, created_at: str):
    for row in rows:
        source = row.path
        source_file = PurePosixPath(source).name
        extension = PurePosixPath(source_file).suffix.lower()
        doc_id = _doc_id(source_file)
        content = bytes(row.content)
        content_hash = hashlib.sha256(content).hexdigest()

        if extension not in SUPPORTED_EXTENSIONS:
            yield _error_row(
                source=source,
                source_file=source_file,
                doc_id=doc_id,
                extension=extension,
                file_size=row.length,
                content_hash=content_hash,
                error=f"unsupported_extension: {extension or 'none'}",
                created_at=created_at,
            )
            continue

        try:
            pages = extract_pages(content=content, extension=extension)
        except Exception as exc:
            yield _error_row(
                source=source,
                source_file=source_file,
                doc_id=doc_id,
                extension=extension,
                file_size=row.length,
                content_hash=content_hash,
                error=f"document_read_error: {exc}",
                created_at=created_at,
            )
            continue

        chunk_index = 0
        for page_index, raw_text in pages:
            text = clean_text(raw_text)
            if not text or len(text) < 20:
                continue

            for text_chunk in split_text(text, chunk_size=chunk_size, chunk_overlap=chunk_overlap):
                yield Row(
                    doc_id=doc_id,
                    source=source,
                    source_file=source_file,
                    source_extension=extension.lstrip("."),
                    file_size=row.length,
                    content_hash=content_hash,
                    page=page_index,
                    chunk_id=f"{doc_id}-{page_index}-{chunk_index}",
                    chunk_index=chunk_index,
                    chunk_text=text_chunk,
                    chunk_text_length=len(text_chunk),
                    chunk_size=chunk_size,
                    chunk_overlap=chunk_overlap,
                    doc_type=infer_doc_type(extension),
                    data_layer="silver",
                    created_at=created_at,
                    status="ok",
                    error=None,
                )
                chunk_index += 1


def extract_pages(content: bytes, extension: str) -> list[tuple[int, str]]:
    if extension == ".pdf":
        return extract_pdf_pages(content)

    text = decode_text(content)
    if extension in {".html", ".htm"}:
        text = strip_html(text)

    return [(0, text)]


def extract_pdf_pages(content: bytes) -> list[tuple[int, str]]:
    from pypdf import PdfReader

    reader = PdfReader(BytesIO(content))
    pages: list[tuple[int, str]] = []
    for page_index, page in enumerate(reader.pages):
        try:
            pages.append((page_index, page.extract_text() or ""))
        except Exception as exc:
            pages.append((page_index, f""))
            print(f"page_extract_error page={page_index}: {exc}")
    return pages


def decode_text(content: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="ignore")


def strip_html(text: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return text


def clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def split_text(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if chunk_overlap < 0 or chunk_overlap >= chunk_size:
        raise ValueError("chunk_overlap must be >= 0 and smaller than chunk_size")

    pieces = _recursive_split(text, SEPARATORS, max_size=chunk_size)
    chunks: list[str] = []
    current = ""

    for piece in pieces:
        candidate = f"{current} {piece}".strip() if current else piece.strip()
        if len(candidate) <= chunk_size:
            current = candidate
            continue

        if current:
            chunks.append(current)
            current = _overlap_suffix(current, chunk_overlap)

        if len(piece) > chunk_size:
            for start in range(0, len(piece), chunk_size - chunk_overlap):
                chunk = piece[start : start + chunk_size].strip()
                if chunk:
                    chunks.append(chunk)
            current = ""
        else:
            current = f"{current} {piece}".strip() if current else piece.strip()

    if current:
        chunks.append(current)

    return chunks


def _recursive_split(text: str, separators: tuple[str, ...], max_size: int) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if not separators:
        return [text]

    separator = separators[0]
    parts = [part.strip() for part in text.split(separator) if part.strip()]
    if len(parts) == 1:
        return _recursive_split(text, separators[1:], max_size=max_size)

    split_parts: list[str] = []
    for part in parts:
        if len(part) > max_size:
            split_parts.extend(_recursive_split(part, separators[1:], max_size=max_size))
        else:
            split_parts.append(part)
    return split_parts


def _overlap_suffix(text: str, chunk_overlap: int) -> str:
    if chunk_overlap == 0:
        return ""
    return text[-chunk_overlap:].strip()


def _doc_id(source_file: str) -> str:
    stem = PurePosixPath(source_file).stem.lower()
    return re.sub(r"[^a-z0-9]+", "-", stem).strip("-") or "document"


def infer_doc_type(extension: str) -> str:
    if extension == ".pdf":
        return "clinical_pdf"
    if extension in {".html", ".htm"}:
        return "web_document"
    if extension == ".csv":
        return "csv_document"
    return "text_document"


def _error_row(
    *,
    source: str,
    source_file: str,
    doc_id: str,
    extension: str,
    file_size: int,
    content_hash: str,
    error: str,
    created_at: str,
    page: int | None = None,
) -> Row:
    return Row(
        doc_id=doc_id,
        source=source,
        source_file=source_file,
        source_extension=extension.lstrip("."),
        file_size=file_size,
        content_hash=content_hash,
        page=page,
        chunk_id=None,
        chunk_index=None,
        chunk_text=None,
        chunk_text_length=0,
        chunk_size=None,
        chunk_overlap=None,
        doc_type=infer_doc_type(extension),
        data_layer="silver",
        created_at=created_at,
        status="error",
        error=error,
    )


def output_schema() -> StructType:
    return StructType(
        [
            StructField("doc_id", StringType(), False),
            StructField("source", StringType(), False),
            StructField("source_file", StringType(), False),
            StructField("source_extension", StringType(), False),
            StructField("file_size", LongType(), False),
            StructField("content_hash", StringType(), False),
            StructField("page", IntegerType(), True),
            StructField("chunk_id", StringType(), True),
            StructField("chunk_index", IntegerType(), True),
            StructField("chunk_text", StringType(), True),
            StructField("chunk_text_length", IntegerType(), False),
            StructField("chunk_size", IntegerType(), True),
            StructField("chunk_overlap", IntegerType(), True),
            StructField("doc_type", StringType(), False),
            StructField("data_layer", StringType(), False),
            StructField("created_at", StringType(), False),
            StructField("status", StringType(), False),
            StructField("error", StringType(), True),
        ]
    )


if __name__ == "__main__":
    main()
