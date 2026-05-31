"""Distributed Bronze-to-Silver processing for chatbot JSONL logs using PySpark."""

from typing import List

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
PROCESS_DATE = "2026-05-21"

BRONZE_INPUT_PREFIX = (
    f"gs://{BUCKET_NAME}/bronze/chat_logs/date={PROCESS_DATE}/"
)
BRONZE_INPUT_PATH = f"{BRONZE_INPUT_PREFIX}*.jsonl"
SILVER_OUTPUT_PATH = (
    f"gs://{BUCKET_NAME}/silver/anonymized_chat/date={PROCESS_DATE}/"
)

DRY_RUN = True
OUTPUT_FORMAT = "json"  # Spark JSON output is JSON Lines in distributed part files.
WRITE_MODE = "errorifexists"  # Allowed: errorifexists, append, overwrite.
ALLOW_OVERWRITE = False  # Set True only after explicitly approving replacement data.

EMAIL_REGEX = r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b"
PHONE_REGEX = r"(?<!\w)(?:\+?\d[\d\s().-]{7,}\d)(?!\w)"

HIGH_RISK_REGEX = r"(?i)(\bkill\b|\bmurder\b|\bsuicide\b|hurt someone|harm someone|self harm)"
MEDIUM_RISK_REGEX = r"(?i)(\bsad\b|\bstress\b|\bdepress\w*\b|\banxiety\b|\bpanic\b|\bhopeless\b)"
NEGATIVE_REGEX = r"(?i)(\bsad\b|\bstress\b|\bdepress\w*\b|\banxiety\b|\bpanic\b|\bkill\b|\bsuicide\b|\bhurt\b|\bharm\b|\bhopeless\b)"
POSITIVE_REGEX = r"(?i)(\bhappy\b|\bgood\b|\bgreat\b|\bthanks\b|thank you|\bbetter\b)"
HARM_INTENT_REGEX = r"(?i)(\bkill\b|\bmurder\b|hurt someone|harm someone)"
SELF_HARM_REGEX = r"(?i)(\bsuicide\b|self harm)"


SOURCE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("anonymous_session_id", StringType(), True),
        StructField("question", StringType(), True),
        StructField("answer", StringType(), True),
        StructField("standalone_query", StringType(), True),
        StructField("model", StringType(), True),
        StructField("is_document_rag", BooleanType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)

OUTPUT_COLUMNS: List[str] = [
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
    "has_question",
    "has_answer",
    "is_valid",
    "processed_at",
]


def build_spark_session() -> SparkSession:
    """Create a Spark session; GCS connector must be available at submit time."""
    return (
        SparkSession.builder.appName("bronze_to_silver_anonymized_chat")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def clean_and_mask_text(column_name: str) -> Column:
    """Normalize whitespace and mask common contact details in a text column."""
    normalized = F.trim(
        F.regexp_replace(
            F.regexp_replace(F.coalesce(F.col(column_name), F.lit("")), r"[\r\n]+", " "),
            r"\s+",
            " ",
        )
    )
    email_masked = F.regexp_replace(normalized, EMAIL_REGEX, "[EMAIL]")
    return F.regexp_replace(email_masked, PHONE_REGEX, "[PHONE]")


def read_bronze_logs(spark: SparkSession) -> DataFrame:
    """Read every JSONL object from the selected Bronze date partition."""
    return (
        spark.read.schema(SOURCE_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(BRONZE_INPUT_PATH)
    )


def build_silver_dataframe(raw_df: DataFrame) -> DataFrame:
    """Validate, deduplicate, normalize, classify, and mask chat events."""
    parsed_df = (
        raw_df.filter(F.col("_corrupt_record").isNull())
        .withColumn("event_id", F.trim(F.col("event_id")))
        .withColumn("timestamp_parsed", F.to_timestamp(F.col("timestamp")))
        .filter(
            (F.col("event_id").isNotNull())
            & (F.length(F.col("event_id")) > 0)
            & F.col("timestamp_parsed").isNotNull()
        )
    )

    deduplicated_df = parsed_df.dropDuplicates(["event_id"])
    cleaned_df = (
        deduplicated_df.withColumn("question_clean", clean_and_mask_text("question"))
        .withColumn("answer_clean", clean_and_mask_text("answer"))
        .withColumn("standalone_query_clean", clean_and_mask_text("standalone_query"))
        .withColumn("is_document_rag", F.coalesce(F.col("is_document_rag"), F.lit(False)))
    )

    feature_df = (
        cleaned_df.withColumn(
            "risk_level",
            F.when(F.col("question_clean").rlike(HIGH_RISK_REGEX), F.lit("high"))
            .when(F.col("question_clean").rlike(MEDIUM_RISK_REGEX), F.lit("medium"))
            .otherwise(F.lit("low")),
        )
        .withColumn(
            "sentiment",
            F.when(F.col("question_clean").rlike(NEGATIVE_REGEX), F.lit("negative"))
            .when(F.col("question_clean").rlike(POSITIVE_REGEX), F.lit("positive"))
            .otherwise(F.lit("neutral")),
        )
        .withColumn(
            "topic",
            F.when(F.col("question_clean").rlike(HARM_INTENT_REGEX), F.lit("harm_intent"))
            .when(F.col("question_clean").rlike(SELF_HARM_REGEX), F.lit("self_harm"))
            .when(F.col("question_clean").rlike(MEDIUM_RISK_REGEX), F.lit("mental_health"))
            .when(F.col("is_document_rag"), F.lit("rag_question"))
            .otherwise(F.lit("general")),
        )
    )

    return (
        feature_df.withColumn("timestamp", F.col("timestamp_parsed"))
        .withColumn("date", F.to_date(F.col("timestamp_parsed")))
        .withColumn("year", F.year(F.col("timestamp_parsed")))
        .withColumn("month", F.month(F.col("timestamp_parsed")))
        .withColumn("day", F.dayofmonth(F.col("timestamp_parsed")))
        .withColumn("hour", F.hour(F.col("timestamp_parsed")))
        .withColumn("question_length", F.length(F.col("question_clean")))
        .withColumn("answer_length", F.length(F.col("answer_clean")))
        .withColumn("standalone_query_length", F.length(F.col("standalone_query_clean")))
        .withColumn("sensitive_flag", F.col("risk_level") == F.lit("high"))
        .withColumn(
            "display_question",
            F.when(F.col("risk_level") == F.lit("high"), F.lit("[HIGH_RISK_CONTENT]"))
            .otherwise(F.col("question_clean")),
        )
        .withColumn("has_question", F.length(F.col("question_clean")) > 0)
        .withColumn("has_answer", F.length(F.col("answer_clean")) > 0)
        .withColumn("is_valid", F.lit(True))
        .withColumn("processed_at", F.current_timestamp())
        .select(*OUTPUT_COLUMNS)
    )


def show_quality_summary(raw_df: DataFrame, silver_df: DataFrame) -> None:
    """Materialize quality counts and sample rows for dry-run verification."""
    valid_required_df = (
        raw_df.filter(F.col("_corrupt_record").isNull())
        .withColumn("event_id_trimmed", F.trim(F.col("event_id")))
        .withColumn("timestamp_parsed", F.to_timestamp(F.col("timestamp")))
        .filter(
            (F.col("event_id_trimmed").isNotNull())
            & (F.length(F.col("event_id_trimmed")) > 0)
            & F.col("timestamp_parsed").isNotNull()
        )
    )
    raw_count = raw_df.count()
    malformed_count = raw_df.filter(F.col("_corrupt_record").isNotNull()).count()
    valid_required_count = valid_required_df.count()
    silver_count = silver_df.count()

    print("\nData quality summary")
    print(f"- Input prefix: {BRONZE_INPUT_PREFIX}")
    print(f"- Total raw records: {raw_count}")
    print(f"- Malformed JSON records skipped: {malformed_count}")
    print(f"- Records after required event_id/timestamp validation: {valid_required_count}")
    print(f"- Records after event_id deduplication: {silver_count}")

    print("\nRisk level summary")
    silver_df.groupBy("risk_level").count().orderBy("risk_level").show(truncate=False)
    print("\nSentiment summary")
    silver_df.groupBy("sentiment").count().orderBy("sentiment").show(truncate=False)
    print("\nTopic summary")
    silver_df.groupBy("topic").count().orderBy("topic").show(truncate=False)
    print("\nSilver sample (10 rows)")
    silver_df.show(10, truncate=False)


def write_silver_output(silver_df: DataFrame) -> None:
    """Write only cleaned output data to the Silver prefix when explicitly enabled."""
    allowed_modes = {"errorifexists", "append", "overwrite"}
    if WRITE_MODE not in allowed_modes:
        raise ValueError(f"Unsupported WRITE_MODE: {WRITE_MODE}")
    if WRITE_MODE == "overwrite" and not ALLOW_OVERWRITE:
        raise ValueError(
            "WRITE_MODE='overwrite' is blocked while ALLOW_OVERWRITE=False. "
            "Confirm replacement data explicitly before enabling overwrite."
        )
    if OUTPUT_FORMAT not in {"json", "parquet"}:
        raise ValueError(f"Unsupported OUTPUT_FORMAT: {OUTPUT_FORMAT}")

    writer = silver_df.write.mode(WRITE_MODE)
    if OUTPUT_FORMAT == "json":
        writer.json(SILVER_OUTPUT_PATH)
    else:
        writer.parquet(SILVER_OUTPUT_PATH)
    print(f"Silver output data written to {SILVER_OUTPUT_PATH}")


def main() -> None:
    spark = build_spark_session()
    raw_df = None
    silver_df = None
    try:
        print(f"Reading all Bronze JSONL files: {BRONZE_INPUT_PATH}")
        raw_df = read_bronze_logs(spark).cache()
        silver_df = build_silver_dataframe(raw_df).cache()
        show_quality_summary(raw_df, silver_df)

        if DRY_RUN:
            print("\nDRY_RUN=True: Silver data was not written to Cloud Storage.")
            print(f"Planned Silver output prefix: {SILVER_OUTPUT_PATH}")
        else:
            write_silver_output(silver_df)
    finally:
        if silver_df is not None:
            silver_df.unpersist()
        if raw_df is not None:
            raw_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
