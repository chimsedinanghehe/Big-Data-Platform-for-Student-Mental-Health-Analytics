"""Chat Bronze -> Silver batch job for Dataproc Serverless Spark.

Scope:
- Read only raw chatbot JSONL events under bronze/chat_logs.
- Clean, validate, deduplicate, anonymize, and enrich record-level chat logs.
- Write only cleaned/anonymized record-level Parquet to silver/anonymized_chat.
- Do not read or write survey, knowledge base, vector, embeddings, or RAG chunks.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta
from functools import reduce
from typing import List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
BRONZE_CHAT_PATH = "gs://student-mental-health-lake-nhom1-2026/bronze/chat_logs/"
SILVER_CHAT_PATH = "gs://student-mental-health-lake-nhom1-2026/silver/anonymized_chat/"
INVALID_CHAT_PATH = "gs://student-mental-health-lake-nhom1-2026/silver/anonymized_chat_invalid/"
WRITE_MODE = "overwrite"

CHAT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("anonymous_session_id", T.StringType(), True),
        T.StructField("question", T.StringType(), True),
        T.StructField("answer", T.StringType(), True),
        T.StructField("standalone_query", T.StringType(), True),
        T.StructField("model", T.StringType(), True),
        T.StructField("is_document_rag", T.BooleanType(), True),
    ]
)

OUTPUT_COLUMNS = [
    "event_id",
    "event_type",
    "timestamp",
    "date",
    "year",
    "month",
    "day",
    "hour",
    "anonymous_session_id",
    "question_clean",
    "answer_clean",
    "standalone_query_clean",
    "display_question",
    "model",
    "is_document_rag",
    "question_length",
    "answer_length",
    "standalone_query_length",
    "risk_level",
    "sentiment",
    "topic",
    "sensitive_flag",
    "processed_at",
    "source_layer",
    "target_layer",
    "is_valid",
]

HIGH_RISK_PATTERN = r"(?i)(\bkill\b|\bmurder\b|\bsuicide\b|hurt\s+someone|harm\s+someone|self\s*harm)"
MEDIUM_RISK_PATTERN = r"(?i)(\bsad\b|\bstress\b|\bstressed\b|\bdepress\w*\b|\banxiety\b|\bpanic\b|\bhopeless\b)"
NEGATIVE_PATTERN = r"(?i)(\bsad\b|\bstress\b|\bstressed\b|\bdepress\w*\b|\banxiety\b|\bpanic\b|\bkill\b|\bsuicide\b|\bhurt\b|\bharm\b|\bhopeless\b)"
POSITIVE_PATTERN = r"(?i)(\bhappy\b|\bgood\b|\bgreat\b|\bthanks\b|thank\s+you|\bbetter\b)"
HARM_INTENT_PATTERN = r"(?i)(\bkill\b|\bmurder\b|hurt\s+someone|harm\s+someone)"
SELF_HARM_PATTERN = r"(?i)(\bsuicide\b|self\s*harm)"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean and anonymize Bronze chatbot JSONL logs into Silver Parquet.")
    parser.add_argument("--input-path", default=BRONZE_CHAT_PATH)
    parser.add_argument("--output-path", default=SILVER_CHAT_PATH)
    parser.add_argument("--invalid-output-path", default=INVALID_CHAT_PATH)
    parser.add_argument("--process-date", help="Read only bronze/chat_logs/date=YYYY-MM-DD/ for backfill or rerun.")
    parser.add_argument("--start-date", help="Read only date partitions from this YYYY-MM-DD date, inclusive.")
    parser.add_argument("--end-date", help="Read only date partitions through this YYYY-MM-DD date, inclusive.")
    parser.add_argument("--output-partitions", type=int, default=16)
    parser.add_argument("--write-mode", default=WRITE_MODE, choices=["overwrite", "append", "errorifexists", "ignore"])
    args = parser.parse_args()
    if args.process_date and (args.start_date or args.end_date):
        parser.error("--process-date cannot be combined with --start-date/--end-date")
    if bool(args.start_date) != bool(args.end_date):
        parser.error("--start-date and --end-date must be provided together")
    return args


def parse_yyyy_mm_dd(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def resolve_input_paths(input_path: str, process_date: Optional[str], start_date: Optional[str], end_date: Optional[str]) -> Union[str, List[str]]:
    base = input_path.rstrip("/")
    if process_date:
        parse_yyyy_mm_dd(process_date)
        return f"{base}/date={process_date}/"

    if start_date and end_date:
        start = parse_yyyy_mm_dd(start_date)
        end = parse_yyyy_mm_dd(end_date)
        if start > end:
            raise ValueError("--start-date must be earlier than or equal to --end-date")
        paths: List[str] = []
        current = start
        while current <= end:
            paths.append(f"{base}/date={current.isoformat()}/")
            current += timedelta(days=1)
        return paths

    return input_path


def print_json_log(payload: dict) -> None:
    print("JOB_JSON_LOG " + json.dumps(payload, default=str, sort_keys=True))


def reduce_boolean_and(expressions):
    values = list(expressions)
    if not values:
        return F.lit(True)
    return reduce(lambda left, right: left & right, values)


def clean_text(column_name: str):
    value = F.coalesce(F.col(column_name).cast("string"), F.lit(""))
    value = F.regexp_replace(value, r"[\r\n]+", " ")
    value = F.regexp_replace(value, r"(?i)[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", "[EMAIL]")
    value = F.regexp_replace(value, r"(?<!\d)(?:\+?\d[\d\s().\-]{7,}\d)(?!\d)", "[PHONE]")
    value = F.regexp_replace(value, r"\s+", " ")
    return F.trim(value)


def count_by(df: DataFrame, column_name: str, limit: int = 100) -> None:
    if column_name not in df.columns:
        print(f"WARNING: Cannot count by missing column {column_name}")
        return
    print(f"\nCOUNT BY {column_name}")
    df.groupBy(column_name).count().orderBy(column_name).show(limit, truncate=False)


def parse_raw_events(raw: DataFrame) -> DataFrame:
    parsed = raw.select(
        F.from_json(F.col("value"), CHAT_SCHEMA).alias("event"),
        F.col("value").alias("_raw_json"),
        F.input_file_name().alias("_source_file"),
    )
    schema_fields_missing = reduce_boolean_and([F.col(name).isNull() for name in CHAT_SCHEMA.fieldNames()])
    return (
        parsed.withColumn("_json_parse_failed", F.col("event").isNull())
        .select("event.*", "_raw_json", "_source_file", "_json_parse_failed")
        .withColumn("_timestamp_parsed", F.to_timestamp("timestamp"))
        .withColumn(
            "error_reason",
            F.when(F.col("_json_parse_failed"), F.lit("invalid_json"))
            .when((F.col("_json_parse_failed") == F.lit(False)) & schema_fields_missing, F.lit("schema_mismatch"))
            .when(F.col("event_id").isNull() | (F.length(F.trim(F.col("event_id"))) == 0), F.lit("missing_event_id"))
            .when(F.col("timestamp").isNull() | (F.length(F.trim(F.col("timestamp"))) == 0), F.lit("missing_timestamp"))
            .when(F.col("_timestamp_parsed").isNull(), F.lit("timestamp_parse_error")),
        )
    )


def invalid_records(parsed: DataFrame) -> DataFrame:
    return (
        parsed.where(F.col("error_reason").isNotNull())
        .select(
            F.col("_source_file").alias("source_file"),
            F.col("error_reason"),
            F.col("_raw_json").alias("raw_payload"),
            F.current_timestamp().alias("processed_at"),
        )
    )


def build_silver(valid: DataFrame) -> DataFrame:

    deduped = valid.dropDuplicates(["event_id"])

    with_text = (
        deduped.withColumn("question_clean", clean_text("question"))
        .withColumn("answer_clean", clean_text("answer"))
        .withColumn("standalone_query_clean", clean_text("standalone_query"))
    )

    q = F.col("question_clean")
    is_high = q.rlike(HIGH_RISK_PATTERN)
    is_medium = q.rlike(MEDIUM_RISK_PATTERN)
    is_harm_intent = q.rlike(HARM_INTENT_PATTERN)
    is_self_harm = q.rlike(SELF_HARM_PATTERN)
    is_mental_health = q.rlike(MEDIUM_RISK_PATTERN)

    result = (
        with_text.withColumn("timestamp", F.col("_timestamp_parsed"))
        .withColumn("date", F.to_date("timestamp"))
        .withColumn("year", F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn("day", F.dayofmonth("timestamp"))
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("event_type", F.coalesce(F.col("event_type"), F.lit("unknown")))
        .withColumn("anonymous_session_id", F.coalesce(F.col("anonymous_session_id"), F.lit("unknown")))
        .withColumn("model", F.coalesce(F.col("model"), F.lit("unknown")))
        .withColumn("is_document_rag", F.coalesce(F.col("is_document_rag"), F.lit(False)))
        .withColumn("question_length", F.length("question_clean"))
        .withColumn("answer_length", F.length("answer_clean"))
        .withColumn("standalone_query_length", F.length("standalone_query_clean"))
        .withColumn("risk_level", F.when(is_high, "high").when(is_medium, "medium").otherwise("low"))
        .withColumn(
            "sentiment",
            F.when(q.rlike(NEGATIVE_PATTERN), "negative")
            .when(q.rlike(POSITIVE_PATTERN), "positive")
            .otherwise("neutral"),
        )
        .withColumn(
            "topic",
            F.when(is_harm_intent, "harm_intent")
            .when(is_self_harm, "self_harm")
            .when(is_mental_health, "mental_health")
            .when(F.col("is_document_rag") == F.lit(True), "rag_question")
            .otherwise("general"),
        )
        .withColumn(
            "sensitive_flag",
            (F.col("risk_level") == "high") | F.col("topic").isin("harm_intent", "self_harm"),
        )
        .withColumn(
            "display_question",
            F.when(F.col("sensitive_flag") == F.lit(True), F.lit("[HIGH_RISK_CONTENT]")).otherwise(F.col("question_clean")),
        )
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("source_layer", F.lit("bronze"))
        .withColumn("target_layer", F.lit("silver"))
        .withColumn("is_valid", F.lit(True))
    )
    return result.select(*OUTPUT_COLUMNS)


def print_quality_log(silver: DataFrame, raw_count: int, valid_count: int, invalid_count: int, silver_count: int) -> None:
    print("\nDATA QUALITY LOG: CHAT BRONZE -> SILVER")
    print(f"total raw rows: {raw_count}")
    print(f"valid rows before dedup: {valid_count}")
    print(f"invalid rows: {invalid_count}")
    print(f"total rows after cleaning: {silver_count}")
    print(f"duplicate rows removed: {valid_count - silver_count}")
    print("\nSilver schema:")
    silver.printSchema()
    for column_name in ["date", "risk_level", "sentiment", "topic", "model"]:
        count_by(silver, column_name)
    print("\nSAMPLE 20 SILVER ROWS")
    sample_columns = [
        "event_id",
        "timestamp",
        "date",
        "hour",
        "anonymous_session_id",
        "display_question",
        "model",
        "is_document_rag",
        "risk_level",
        "sentiment",
        "topic",
        "sensitive_flag",
    ]
    silver.select(*sample_columns).show(20, truncate=80)


def main() -> None:
    args = parse_args()
    start_time = time.time()
    input_paths = resolve_input_paths(args.input_path, args.process_date, args.start_date, args.end_date)
    spark = (
        SparkSession.builder.appName("chat-bronze-to-silver-anonymized")
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.default.parallelism", "24")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Reading Bronze chat JSONL only from: {input_paths}")
        raw = spark.read.option("recursiveFileLookup", "true").text(input_paths)
        raw_count = raw.count()
        parsed = parse_raw_events(raw)
        invalid = invalid_records(parsed)
        invalid_count = invalid.count()
        valid_input = parsed.where(F.col("error_reason").isNull())
        valid_count = valid_input.count()

        if invalid_count > 0:
            print(f"Writing invalid chat records to {args.invalid_output_path} with mode={args.write_mode}")
            invalid.write.mode(args.write_mode).parquet(args.invalid_output_path)
        else:
            print(f"No invalid chat records detected; invalid path not written: {args.invalid_output_path}")

        silver = build_silver(valid_input)
        silver_count = silver.count()

        output_partitions = max(1, min(args.output_partitions, silver.select("date", "hour").distinct().count()))
        print(f"Writing Silver Parquet to {args.output_path} with mode={args.write_mode}, partitions={output_partitions}")
        silver.repartition(output_partitions, "date", "hour").write.mode(args.write_mode).partitionBy("date", "hour").parquet(args.output_path)
        print_quality_log(silver, raw_count, valid_count, invalid_count, silver_count)
        print_json_log(
            {
                "job_name": "chat_bronze_to_silver",
                "project_id": PROJECT_ID,
                "input_path": input_paths,
                "output_path": args.output_path,
                "invalid_output_path": args.invalid_output_path,
                "write_mode": args.write_mode,
                "input_rows": raw_count,
                "valid_rows": valid_count,
                "invalid_rows": invalid_count,
                "duplicate_removed": valid_count - silver_count,
                "output_rows": silver_count,
                "output_success": True,
                "duration_seconds": round(time.time() - start_time, 2),
                "status": "success",
            }
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
