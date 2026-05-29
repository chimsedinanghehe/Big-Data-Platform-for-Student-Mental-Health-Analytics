from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
from io import BytesIO
import re
from pathlib import PurePosixPath
from uuid import NAMESPACE_URL, uuid5

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType


SEPARATORS = ("\n\n", "\n", ". ", "? ", "! ", "; ", " ")
PAGE_NUMBER_PATTERN = re.compile(r"^(?:page\s*)?\d{1,4}(?:\s*/\s*\d{1,4})?$", re.IGNORECASE)
SUPPORTED_EXTENSIONS = {".pdf", ".html", ".htm", ".txt", ".md"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract and clean knowledge base files from GCS Bronze to Silver.")
    parser.add_argument("--input_path", required=True, help="GCS Bronze path, for example gs://bucket/bronze/knowledge_base/")
    parser.add_argument(
        "--output_path",
        required=True,
        help="GCS Silver path, for example gs://bucket/silver/documents_clean/date=2026-05-28/",
    )
    parser.add_argument("--output_mode", choices=("errorifexists", "overwrite", "append"), default="errorifexists")
    parser.add_argument(
        "--output_format",
        choices=("parquet", "jsonl", "both"),
        default="parquet",
        help="Write Parquet, JSON Lines, or both. With 'both', output_path contains parquet/ and jsonl/ subfolders.",
    )
    parser.add_argument("--min_text_length", type=int, default=20)
    parser.add_argument("--emit_chunks", action="store_true", help="Write chunk-level rows instead of page-level rows.")
    parser.add_argument("--chunk_size", type=int, default=500)
    parser.add_argument("--chunk_overlap", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("knowledge-base-bronze-to-silver").getOrCreate()
    processed_at = datetime.now(UTC).replace(tzinfo=None)

    source_files = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load(args.input_path.rstrip("/"))
        .select("path", "content", "length", "modificationTime")
    )

    rows_rdd = source_files.rdd.mapPartitions(
        lambda partition: extract_partition_pages(
            partition,
            processed_at=processed_at,
            min_text_length=args.min_text_length,
            emit_chunks=args.emit_chunks,
            chunk_size=args.chunk_size,
            chunk_overlap=args.chunk_overlap,
        )
    )

    silver_df = spark.createDataFrame(rows_rdd, schema=silver_schema(emit_chunks=args.emit_chunks))
    write_output(silver_df, args.output_path.rstrip("/"), args.output_mode, args.output_format)
    spark.stop()


def write_output(df, output_path: str, output_mode: str, output_format: str) -> None:
    if output_format == "parquet":
        df.write.mode(output_mode).parquet(output_path)
        return

    if output_format == "jsonl":
        df.write.mode(output_mode).json(output_path)
        return

    df.write.mode(output_mode).parquet(f"{output_path}/parquet")
    df.write.mode(output_mode).json(f"{output_path}/jsonl")


def extract_partition_pages(
    partition,
    processed_at: datetime,
    min_text_length: int,
    emit_chunks: bool,
    chunk_size: int,
    chunk_overlap: int,
):
    from pypdf import PdfReader

    for file_row in partition:
        source_path = file_row.path
        source_file = PurePosixPath(source_path).name
        extension = PurePosixPath(source_file).suffix.lower()
        document_id = stable_document_id(source_path)
        file_hash = hashlib.sha256(bytes(file_row.content)).hexdigest()

        if extension not in SUPPORTED_EXTENSIONS:
            yield error_row(
                document_id=document_id,
                source_path=source_path,
                source_file=source_file,
                file_hash=file_hash,
                file_size=file_row.length,
                processed_at=processed_at,
                error=f"unsupported_extension: {extension or 'none'}",
                emit_chunks=emit_chunks,
            )
            continue

        if file_row.length == 0:
            yield error_row(
                document_id=document_id,
                source_path=source_path,
                source_file=source_file,
                file_hash=file_hash,
                file_size=file_row.length,
                processed_at=processed_at,
                error="empty_file",
                emit_chunks=emit_chunks,
            )
            continue

        if extension == ".pdf":
            try:
                reader = PdfReader(BytesIO(bytes(file_row.content)))
            except Exception as exc:
                yield error_row(
                    document_id=document_id,
                    source_path=source_path,
                    source_file=source_file,
                    file_hash=file_hash,
                    file_size=file_row.length,
                    processed_at=processed_at,
                    error=f"pdf_read_error: {exc}",
                    emit_chunks=emit_chunks,
                )
                continue

            page_rows: list[tuple[int, str | None, str | None]] = []
            for page_index, page in enumerate(reader.pages, start=1):
                try:
                    raw_text = page.extract_text() or ""
                except Exception as exc:
                    page_rows.append((page_index, None, f"page_extract_error: {exc}"))
                else:
                    page_rows.append((page_index, raw_text, None))
        else:
            try:
                text = decode_text(bytes(file_row.content))
                if extension in {".html", ".htm"}:
                    text = strip_html(text)
            except Exception as exc:
                yield error_row(
                    document_id=document_id,
                    source_path=source_path,
                    source_file=source_file,
                    file_hash=file_hash,
                    file_size=file_row.length,
                    processed_at=processed_at,
                    error=f"text_read_error: {exc}",
                    emit_chunks=emit_chunks,
                )
                continue
            page_rows = [(1, text, None)]

        repeated_lines = find_document_repeated_lines(
            [raw_text for _, raw_text, error in page_rows if error is None and raw_text]
        )

        for page_index, raw_text, page_error in page_rows:
            if page_error:
                yield error_row(
                    document_id=document_id,
                    source_path=source_path,
                    source_file=source_file,
                    file_hash=file_hash,
                    file_size=file_row.length,
                    processed_at=processed_at,
                    page_number=page_index,
                    error=page_error,
                    emit_chunks=emit_chunks,
                )
                continue

            clean = clean_text(raw_text, repeated_lines=repeated_lines)
            if len(clean) < min_text_length:
                continue

            if emit_chunks:
                for chunk_index, chunk_text in enumerate(
                    split_text(clean, chunk_size=chunk_size, chunk_overlap=chunk_overlap)
                ):
                    yield Row(
                        document_id=document_id,
                        source_path=source_path,
                        source_file=source_file,
                        file_hash=file_hash,
                        file_size=file_row.length,
                        page_number=page_index,
                        chunk_id=stable_chunk_id(document_id, page_index, chunk_index),
                        chunk_index=chunk_index,
                        chunk_text=chunk_text,
                        chunk_text_length=len(chunk_text),
                        chunk_size=chunk_size,
                        chunk_overlap=chunk_overlap,
                        language=detect_language(chunk_text),
                        processed_at=processed_at,
                        status="ok",
                        error=None,
                    )
            else:
                yield Row(
                    document_id=document_id,
                    source_path=source_path,
                    source_file=source_file,
                    file_hash=file_hash,
                    file_size=file_row.length,
                    page_number=page_index,
                    clean_text=clean,
                    text_length=len(clean),
                    language=detect_language(clean),
                    processed_at=processed_at,
                    status="ok",
                    error=None,
                )


def clean_text(text: str | None, repeated_lines: set[str] | None = None) -> str:
    if not text:
        return ""

    text = text.replace("\x00", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)
    text = remove_repeated_lines(text, repeated_lines=repeated_lines)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[\u0000-\u0008\u000b\u000c\u000e-\u001f]", " ", text)
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    return text.strip()


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
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
    )
    return text


def split_text(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if chunk_overlap < 0 or chunk_overlap >= chunk_size:
        raise ValueError("chunk_overlap must be >= 0 and smaller than chunk_size")

    pieces = recursive_split(text, SEPARATORS, max_size=chunk_size)
    chunks: list[str] = []
    current = ""

    for piece in pieces:
        candidate = f"{current} {piece}".strip() if current else piece.strip()
        if len(candidate) <= chunk_size:
            current = candidate
            continue

        if current:
            chunks.append(current)
            current = overlap_tail(current, chunk_overlap)

        if len(piece) > chunk_size:
            step = chunk_size - chunk_overlap
            for start in range(0, len(piece), step):
                chunk = piece[start : start + chunk_size].strip()
                if chunk:
                    chunks.append(chunk)
            current = ""
        else:
            current = f"{current} {piece}".strip() if current else piece.strip()

    if current:
        chunks.append(current)

    return chunks


def recursive_split(text: str, separators: tuple[str, ...], max_size: int) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= max_size or not separators:
        return [text]

    separator = separators[0]
    raw_parts = text.split(separator)
    parts = []
    for index, part in enumerate(raw_parts):
        part = part.strip()
        if not part:
            continue
        if separator.strip() and index < len(raw_parts) - 1:
            part = f"{part}{separator.strip()}"
        parts.append(part)
    if len(parts) == 1:
        return recursive_split(text, separators[1:], max_size=max_size)

    split_parts: list[str] = []
    for part in parts:
        if len(part) > max_size:
            split_parts.extend(recursive_split(part, separators[1:], max_size=max_size))
        else:
            split_parts.append(part)
    return split_parts


def find_document_repeated_lines(page_texts: list[str]) -> set[str]:
    if len(page_texts) < 3:
        return set()

    counts: dict[str, int] = {}
    for page_text in page_texts:
        page_lines = {
            normalize_line_for_noise(line)
            for line in page_text.splitlines()
            if normalize_line_for_noise(line)
        }
        for normalized in page_lines:
            counts[normalized] = counts.get(normalized, 0) + 1

    threshold = max(3, int(len(page_texts) * 0.35))
    return {
        normalized
        for normalized, count in counts.items()
        if count >= threshold and len(normalized) <= 160
    }


def remove_repeated_lines(text: str, repeated_lines: set[str] | None = None) -> str:
    lines = [line.strip() for line in text.splitlines()]
    non_empty = [line for line in lines if line]
    if not non_empty:
        return ""

    counts: dict[str, int] = {}
    for line in non_empty:
        normalized = line.lower()
        counts[normalized] = counts.get(normalized, 0) + 1

    threshold = max(3, len(non_empty) // 4)
    cleaned = []
    for line in lines:
        normalized = normalize_line_for_noise(line)
        if not line:
            cleaned.append(line)
            continue
        if PAGE_NUMBER_PATTERN.fullmatch(line):
            continue
        if repeated_lines and normalized in repeated_lines:
            continue
        if counts.get(normalized, 0) >= threshold and len(line) < 120:
            continue
        cleaned.append(line)

    return "\n".join(cleaned)


def normalize_line_for_noise(line: str) -> str:
    return re.sub(r"\s+", " ", line.strip().lower())


def overlap_tail(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text.strip()

    tail = text[-max_chars:].strip()
    first_space = tail.find(" ")
    if first_space > 0:
        tail = tail[first_space + 1 :].strip()
    return tail


def stable_document_id(source_path: str) -> str:
    source_file = PurePosixPath(source_path).name
    stem = PurePosixPath(source_file).stem.lower()
    normalized = re.sub(r"[^a-z0-9]+", "-", stem).strip("-") or "document"
    digest = hashlib.sha1(source_path.encode("utf-8")).hexdigest()[:12]
    return f"{normalized}-{digest}"


def stable_chunk_id(document_id: str, page_number: int, chunk_index: int) -> str:
    return str(uuid5(NAMESPACE_URL, f"{document_id}:page={page_number}:chunk={chunk_index}"))


def detect_language(text: str) -> str:
    vietnamese_chars = set("ăâđêôơưáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ")
    sample = text[:2000].lower()
    if any(char in sample for char in vietnamese_chars):
        return "vi"
    return "en"


def error_row(
    *,
    document_id: str,
    source_path: str,
    source_file: str,
    file_hash: str,
    file_size: int,
    processed_at: datetime,
    error: str,
    emit_chunks: bool,
    page_number: int | None = None,
) -> Row:
    if emit_chunks:
        return Row(
            document_id=document_id,
            source_path=source_path,
            source_file=source_file,
            file_hash=file_hash,
            file_size=file_size,
            page_number=page_number,
            chunk_id=None,
            chunk_index=None,
            chunk_text=None,
            chunk_text_length=0,
            chunk_size=None,
            chunk_overlap=None,
            language=None,
            processed_at=processed_at,
            status="error",
            error=error,
        )

    return Row(
        document_id=document_id,
        source_path=source_path,
        source_file=source_file,
        file_hash=file_hash,
        file_size=file_size,
        page_number=page_number,
        clean_text=None,
        text_length=0,
        language=None,
        processed_at=processed_at,
        status="error",
        error=error,
    )


def silver_schema(emit_chunks: bool = False) -> StructType:
    common_fields = [
        StructField("document_id", StringType(), False),
        StructField("source_path", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("file_hash", StringType(), False),
        StructField("file_size", LongType(), False),
        StructField("page_number", IntegerType(), True),
    ]

    if emit_chunks:
        body_fields = [
            StructField("chunk_id", StringType(), True),
            StructField("chunk_index", IntegerType(), True),
            StructField("chunk_text", StringType(), True),
            StructField("chunk_text_length", IntegerType(), False),
            StructField("chunk_size", IntegerType(), True),
            StructField("chunk_overlap", IntegerType(), True),
        ]
    else:
        body_fields = [
            StructField("clean_text", StringType(), True),
            StructField("text_length", IntegerType(), False),
        ]

    tail_fields = [
        StructField("language", StringType(), True),
        StructField("processed_at", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("error", StringType(), True),
    ]

    return StructType(common_fields + body_fields + tail_fields)


if __name__ == "__main__":
    main()
