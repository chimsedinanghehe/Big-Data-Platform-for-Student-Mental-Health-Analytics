"""Chat Kafka -> Silver Structured Streaming job.

This production streaming template reads chatbot events from Kafka, applies the
same clean/anonymize logic as the batch Bronze -> Silver job, and writes
record-level Silver Parquet with checkpointing.

Do not run until KAFKA_BOOTSTRAP_SERVERS is configured.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


KAFKA_BOOTSTRAP_SERVERS = "<KAFKA_BOOTSTRAP_SERVERS>"
KAFKA_TOPIC = "chat-events"
CHECKPOINT_PATH = "gs://student-mental-health-lake-nhom1-2026/checkpoints/chat_kafka_to_silver/"
SILVER_CHAT_PATH = "gs://student-mental-health-lake-nhom1-2026/silver/anonymized_chat/"

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

HIGH_RISK_PATTERN = r"(?i)(\bkill\b|\bmurder\b|\bsuicide\b|hurt\s+someone|harm\s+someone|self\s*harm)"
MEDIUM_RISK_PATTERN = r"(?i)(\bsad\b|\bstress\b|\bstressed\b|\bdepress\w*\b|\banxiety\b|\bpanic\b|\bhopeless\b)"
NEGATIVE_PATTERN = r"(?i)(\bsad\b|\bstress\b|\bstressed\b|\bdepress\w*\b|\banxiety\b|\bpanic\b|\bkill\b|\bsuicide\b|\bhurt\b|\bharm\b|\bhopeless\b)"
POSITIVE_PATTERN = r"(?i)(\bhappy\b|\bgood\b|\bgreat\b|\bthanks\b|thank\s+you|\bbetter\b)"
HARM_INTENT_PATTERN = r"(?i)(\bkill\b|\bmurder\b|hurt\s+someone|harm\s+someone)"
SELF_HARM_PATTERN = r"(?i)(\bsuicide\b|self\s*harm)"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream chatbot Kafka events to Silver anonymized chat.")
    parser.add_argument("--kafka-bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--kafka-topic", default=KAFKA_TOPIC)
    parser.add_argument("--checkpoint-path", default=CHECKPOINT_PATH)
    parser.add_argument("--output-path", default=SILVER_CHAT_PATH)
    return parser.parse_args()


def clean_text(column_name: str):
    value = F.coalesce(F.col(column_name).cast("string"), F.lit(""))
    value = F.regexp_replace(value, r"[\r\n]+", " ")
    value = F.regexp_replace(value, r"(?i)[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", "[EMAIL]")
    value = F.regexp_replace(value, r"(?<!\d)(?:\+?\d[\d\s().\-]{7,}\d)(?!\d)", "[PHONE]")
    value = F.regexp_replace(value, r"\s+", " ")
    return F.trim(value)


def clean_chat_events(events):
    parsed = (
        events.select(F.from_json(F.col("value").cast("string"), CHAT_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .where(
            F.col("event_id").isNotNull()
            & (F.length(F.trim(F.col("event_id"))) > 0)
            & F.col("timestamp").isNotNull()
        )
        .withColumn("question_clean", clean_text("question"))
        .withColumn("answer_clean", clean_text("answer"))
        .withColumn("standalone_query_clean", clean_text("standalone_query"))
    )

    q = F.col("question_clean")
    is_high = q.rlike(HIGH_RISK_PATTERN)
    is_medium = q.rlike(MEDIUM_RISK_PATTERN)
    is_harm_intent = q.rlike(HARM_INTENT_PATTERN)
    is_self_harm = q.rlike(SELF_HARM_PATTERN)

    return (
        parsed.withColumn("date", F.to_date("timestamp"))
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
            .when(q.rlike(MEDIUM_RISK_PATTERN), "mental_health")
            .when(F.col("is_document_rag") == F.lit(True), "rag_question")
            .otherwise("general"),
        )
        .withColumn("sensitive_flag", (F.col("risk_level") == "high") | F.col("topic").isin("harm_intent", "self_harm"))
        .withColumn(
            "display_question",
            F.when(F.col("sensitive_flag") == F.lit(True), F.lit("[HIGH_RISK_CONTENT]")).otherwise(F.col("question_clean")),
        )
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("source_layer", F.lit("kafka"))
        .withColumn("target_layer", F.lit("silver"))
        .withColumn("is_valid", F.lit(True))
        .select(
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
        )
    )


def main() -> None:
    args = parse_args()
    if not args.kafka_bootstrap_servers or args.kafka_bootstrap_servers == "<KAFKA_BOOTSTRAP_SERVERS>":
        raise ValueError("Kafka bootstrap servers are not configured. Set --kafka-bootstrap-servers before running.")

    spark = SparkSession.builder.appName("chat-kafka-to-silver-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap_servers)
        .option("subscribe", args.kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    silver = clean_chat_events(kafka_events).withWatermark("timestamp", "24 hours").dropDuplicates(["event_id"])

    query = (
        silver.writeStream.format("parquet")
        .option("checkpointLocation", args.checkpoint_path)
        .option("path", args.output_path)
        .outputMode("append")
        .partitionBy("date", "hour")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
