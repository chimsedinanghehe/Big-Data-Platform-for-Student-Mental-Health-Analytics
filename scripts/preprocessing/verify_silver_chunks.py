from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


REQUIRED_CHUNK_FIELDS = {
    "document_id",
    "source_path",
    "source_file",
    "file_hash",
    "file_size",
    "page_number",
    "chunk_id",
    "chunk_index",
    "chunk_text",
    "chunk_text_length",
    "chunk_size",
    "chunk_overlap",
    "language",
    "processed_at",
    "status",
    "error",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify chunk-level Silver Parquet output on GCS.")
    parser.add_argument("--input_path", required=True, help="GCS Silver chunk path.")
    parser.add_argument("--preview_limit", type=int, default=8)
    parser.add_argument("--fail_on_empty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    spark = SparkSession.builder.appName("verify-silver-chunks").getOrCreate()

    df = spark.read.parquet(args.input_path.rstrip("/"))
    columns = set(df.columns)
    missing = sorted(REQUIRED_CHUNK_FIELDS - columns)

    print(f"silver_output_path={args.input_path}")
    print("schema:")
    df.printSchema()

    row_count = df.count()
    valid_df = df.filter(
        (F.col("status") == F.lit("ok"))
        & F.col("chunk_text").isNotNull()
        & (F.length(F.trim(F.col("chunk_text"))) > 0)
    )
    valid_count = valid_df.count()
    empty_count = df.filter(
        F.col("chunk_text").isNull() | (F.length(F.trim(F.col("chunk_text"))) == 0)
    ).count()
    error_count = df.filter(F.col("status") != F.lit("ok")).count()
    source_count = df.select("source_path").distinct().count() if "source_path" in columns else 0

    print(f"missing_required_fields={missing}")
    print(f"row_count={row_count}")
    print(f"source_file_count={source_count}")
    print(f"valid_chunk_count={valid_count}")
    print(f"empty_chunk_count={empty_count}")
    print(f"error_row_count={error_count}")
    print(f"valid_ratio={(valid_count / row_count) if row_count else 0:.6f}")

    print("status_counts:")
    df.groupBy("status").count().show(truncate=False)

    print("chunk_length_summary:")
    valid_df.select("chunk_text_length").summary("count", "min", "mean", "max").show(truncate=False)

    print("preview:")
    valid_df.select(
        "source_file",
        "page_number",
        "chunk_index",
        "chunk_text_length",
        F.substring("chunk_text", 1, 500).alias("chunk_text_preview"),
    ).show(args.preview_limit, truncate=False)

    spark.stop()

    if missing:
        return 2
    if args.fail_on_empty and row_count == 0:
        return 3
    if args.fail_on_empty and valid_count == 0:
        return 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
