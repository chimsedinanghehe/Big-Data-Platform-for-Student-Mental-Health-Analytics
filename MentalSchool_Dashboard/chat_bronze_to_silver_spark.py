"""Chat Bronze -> Silver batch job for Dataproc Serverless Spark.

Production path:
- Read raw chatbot JSONL events from GCS Bronze.
- Clean, validate, deduplicate, anonymize, and enrich record-level chat logs.
- Preserve audience metadata for dashboard splits by school/university.
- Write only Silver Parquet.

Debug/verification work is gated by flags so production does not scan the same
data repeatedly just for logs.
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
NOT_COUNTED = "not_counted_for_speed"

CHAT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("anonymous_session_id", T.StringType(), True),
        T.StructField("user_id_hash", T.StringType(), True),
        T.StructField("user_age", T.IntegerType(), True),
        T.StructField("user_gender", T.StringType(), True),
        T.StructField("learner_type", T.StringType(), True),
        T.StructField("grade", T.StringType(), True),
        T.StructField("class_level", T.StringType(), True),
        T.StructField("user_group", T.StringType(), True),
        T.StructField("audience_group", T.StringType(), True),
        T.StructField("survey_type", T.StringType(), True),
        T.StructField("survey_completed", T.BooleanType(), True),
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
    "user_id_hash",
    "user_age",
    "user_gender",
    "learner_type",
    "grade",
    "class_level",
    "user_group",
    "audience_group",
    "survey_type",
    "survey_completed",
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

HIGH_RISK_PATTERN = r"(?i)(\bkill\b|\bmurder\b|\bsuicide\b|hurt\s+someone|harm\s+someone|self\s*harm|kill\s+myself|hurt\s+myself|tự\s*tử|tự\s*hại|giết\s+mình|làm\s+hại\s+bản\s+thân|giết\s+người|làm\s+hại\s+người)"
MEDIUM_RISK_PATTERN = r"(?i)(\bsad\b|\bstress\b|\bstressed\b|\bdepress\w*\b|\banxiety\b|\bpanic\b|\bhopeless\b|lonely|burnout|buồn|căng\s*thẳng|trầm\s*cảm|lo\s*âu|hoảng\s*loạn|tuyệt\s*vọng|cô\s*đơn|kiệt\s*sức)"
NEGATIVE_PATTERN = r"(?i)(\bsad\b|\bstress\b|\bstressed\b|\bdepress\w*\b|\banxiety\b|\bpanic\b|\bkill\b|\bsuicide\b|\bhurt\b|\bharm\b|\bhopeless\b|lonely|burnout|buồn|căng\s*thẳng|trầm\s*cảm|lo\s*âu|hoảng\s*loạn|tự\s*tử|tự\s*hại|đau\s*khổ|tuyệt\s*vọng|cô\s*đơn|kiệt\s*sức)"
POSITIVE_PATTERN = r"(?i)(\bhappy\b|\bgood\b|\bgreat\b|\bthanks\b|thank\s+you|\bbetter\b|vui|ổn|tốt|cảm\s*ơn|đỡ\s*hơn|khá\s*hơn)"
HARM_INTENT_PATTERN = r"(?i)(\bkill\b|\bmurder\b|hurt\s+someone|harm\s+someone|giết\s+người|làm\s+hại\s+người|đánh\s+người)"
SELF_HARM_PATTERN = r"(?i)(\bsuicide\b|self\s*harm|kill\s+myself|hurt\s+myself|tự\s*tử|tự\s*hại|giết\s+mình|làm\s+hại\s+bản\s+thân)"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean and anonymize Bronze chatbot JSONL logs into Silver Parquet.")
    parser.add_argument("--input-path", default=BRONZE_CHAT_PATH)
    parser.add_argument("--output-path", default=SILVER_CHAT_PATH)
    parser.add_argument("--invalid-output-path", default=INVALID_CHAT_PATH)
    parser.add_argument("--process-date", help="Read only bronze/chat_logs/date=YYYY-MM-DD/.")
    parser.add_argument("--start-date", help="Read only date partitions from this YYYY-MM-DD date, inclusive.")
    parser.add_argument("--end-date", help="Read only date partitions through this YYYY-MM-DD date, inclusive.")
    parser.add_argument("--output-partitions", type=int, default=4)
    parser.add_argument("--spark-parallelism", type=int, default=8)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--write-mode", default=WRITE_MODE, choices=["overwrite", "append", "errorifexists", "ignore"])
    parser.add_argument("--enable-quality-report", action="store_true")
    parser.add_argument("--enable-output-verify", action="store_true")
    parser.add_argument("--write-invalid-records", action="store_true")
    parser.add_argument("--fast-mode", action="store_true", help="Kept for orchestration clarity; production defaults are already fast.")
    parser.add_argument("--run-id")
    parser.add_argument("--versioned-output", action="store_true")
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


def resolve_output_path(output_path: str, run_id: Optional[str], versioned_output: bool) -> str:
    if versioned_output:
        if not run_id:
            raise ValueError("--run-id is required when --versioned-output is enabled")
        return f"{output_path.rstrip('/')}/run_id={run_id}/"
    return output_path


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


def audience_group_expression():
    raw_group = F.lower(
        F.trim(
            F.coalesce(
                F.col("audience_group").cast("string"),
                F.col("user_group").cast("string"),
                F.col("survey_type").cast("string"),
                F.lit(""),
            )
        )
    )
    age = F.col("user_age").cast("int")
    return (
        F.when(raw_group.isin("school", "student_school", "high_school"), F.lit("school"))
        .when(raw_group.isin("university", "college", "student_university"), F.lit("university"))
        .when(raw_group.rlike("school|hoc sinh|hocsinh"), F.lit("school"))
        .when(raw_group.rlike("university|college|sinh vien|sinhvien"), F.lit("university"))
        .when(age.isNotNull() & (age <= 18), F.lit("school"))
        .when(age.isNotNull() & (age >= 19), F.lit("university"))
        .otherwise(F.lit("unknown"))
    )


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
    expanded = parsed.select("event.*", "_raw_json", "_source_file", F.col("event").isNull().alias("_json_parse_failed"))
    schema_fields_missing = reduce_boolean_and([F.col(name).isNull() for name in CHAT_SCHEMA.fieldNames()])
    return (
        expanded.withColumn("_timestamp_parsed", F.to_timestamp("timestamp"))
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
        .withColumn("user_id_hash", F.col("user_id_hash").cast("string"))
        .withColumn("user_age", F.col("user_age").cast("int"))
        .withColumn("user_gender", F.col("user_gender").cast("string"))
        .withColumn("learner_type", F.col("learner_type").cast("string"))
        .withColumn("grade", F.col("grade").cast("string"))
        .withColumn("class_level", F.coalesce(F.col("class_level").cast("string"), F.col("grade").cast("string"), F.col("learner_type").cast("string")))
        .withColumn("audience_group", audience_group_expression())
        .withColumn("user_group", F.coalesce(F.col("user_group").cast("string"), F.col("audience_group")))
        .withColumn("survey_type", F.coalesce(F.col("survey_type").cast("string"), F.col("audience_group")))
        .withColumn("survey_completed", F.coalesce(F.col("survey_completed").cast("boolean"), F.lit(False)))
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


def print_quality_log(raw: DataFrame, parsed: DataFrame, valid_input: DataFrame, invalid: DataFrame, silver: DataFrame) -> dict:
    raw_count = raw.count()
    valid_count = valid_input.count()
    invalid_count = invalid.count()
    silver_count = silver.count()
    print("\nDATA QUALITY LOG: CHAT BRONZE -> SILVER")
    print(f"total raw rows: {raw_count}")
    print(f"valid rows before dedup: {valid_count}")
    print(f"invalid rows: {invalid_count}")
    print(f"total rows after cleaning: {silver_count}")
    print(f"duplicate rows removed: {valid_count - silver_count}")
    print("\nSilver schema:")
    silver.printSchema()
    for column_name in ["date", "audience_group", "risk_level", "sentiment", "topic", "model"]:
        count_by(silver, column_name)
    print("\nSAMPLE 20 SILVER ROWS")
    sample_columns = [
        "event_id",
        "timestamp",
        "date",
        "hour",
        "audience_group",
        "user_age",
        "user_gender",
        "learner_type",
        "class_level",
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
    return {
        "input_rows": raw_count,
        "valid_rows": valid_count,
        "invalid_rows": invalid_count,
        "duplicate_removed": valid_count - silver_count,
        "output_rows": silver_count,
    }


def main() -> None:
    args = parse_args()
    start_time = time.time()
    input_paths = resolve_input_paths(args.input_path, args.process_date, args.start_date, args.end_date)
    effective_output_path = resolve_output_path(args.output_path, args.run_id, args.versioned_output)
    spark = (
        SparkSession.builder.appName("chat-bronze-to-silver-anonymized")
        .config("spark.sql.shuffle.partitions", str(max(1, args.shuffle_partitions)))
        .config("spark.default.parallelism", str(max(1, args.spark_parallelism)))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    quality_metrics = {
        "input_rows": NOT_COUNTED,
        "valid_rows": NOT_COUNTED,
        "invalid_rows": NOT_COUNTED,
        "duplicate_removed": NOT_COUNTED,
        "output_rows": NOT_COUNTED,
    }
    status = "success"

    try:
        print(f"Reading Bronze chat JSONL from: {input_paths}")
        raw = spark.read.option("recursiveFileLookup", "true").text(input_paths)
        parsed = parse_raw_events(raw)
        invalid = invalid_records(parsed)
        valid_input = parsed.where(F.col("error_reason").isNull())

        if args.write_invalid_records:
            print(f"Writing invalid chat records to {args.invalid_output_path} with mode={args.write_mode}")
            invalid.write.mode(args.write_mode).parquet(args.invalid_output_path)

        silver = build_silver(valid_input)
        writer_df = silver.repartition(max(1, args.output_partitions), "date", "hour")
        print(
            "Writing Silver Parquet to "
            f"{effective_output_path} with mode={args.write_mode}, partitions={max(1, args.output_partitions)}"
        )
        writer_df.write.mode(args.write_mode).partitionBy("date", "hour").parquet(effective_output_path)

        if args.enable_quality_report:
            quality_metrics = print_quality_log(raw, parsed, valid_input, invalid, silver)
        elif args.enable_output_verify:
            quality_metrics["output_rows"] = spark.read.parquet(effective_output_path).count()

        print_json_log(
            {
                "job_name": "chat_bronze_to_silver",
                "project_id": PROJECT_ID,
                "input_path": input_paths,
                "output_path": effective_output_path,
                "base_output_path": args.output_path,
                "invalid_output_path": args.invalid_output_path if args.write_invalid_records else None,
                "write_mode": args.write_mode,
                "run_id": args.run_id,
                "versioned_output": args.versioned_output,
                "quality_report_enabled": args.enable_quality_report,
                "output_verify_enabled": args.enable_output_verify,
                "write_invalid_records": args.write_invalid_records,
                "partition_config": {
                    "output_partitions": max(1, args.output_partitions),
                    "spark_parallelism": max(1, args.spark_parallelism),
                    "shuffle_partitions": max(1, args.shuffle_partitions),
                },
                **quality_metrics,
                "output_success": True,
                "duration_seconds": round(time.time() - start_time, 2),
                "status": status,
            }
        )
    except Exception:
        status = "failed"
        print_json_log(
            {
                "job_name": "chat_bronze_to_silver",
                "project_id": PROJECT_ID,
                "input_path": input_paths,
                "output_path": effective_output_path,
                "output_success": False,
                "duration_seconds": round(time.time() - start_time, 2),
                "status": status,
            }
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
