"""Chat Silver stream -> realtime Gold metrics Structured Streaming job.

Reads cleaned/anonymized chat records from Silver, creates windowed metrics, and
writes dashboard-ready realtime Gold metrics. This job does not read Bronze.
"""

from __future__ import annotations

import argparse
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


SILVER_CHAT_PATH = "gs://student-mental-health-lake-nhom1-2026/silver/anonymized_chat/"
GOLD_REALTIME_METRICS_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_realtime_metrics/"
CHECKPOINT_PATH = "gs://student-mental-health-lake-nhom1-2026/checkpoints/chat_streaming_to_gold/"

SILVER_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("anonymous_session_id", T.StringType(), True),
        T.StructField("question_clean", T.StringType(), True),
        T.StructField("answer_clean", T.StringType(), True),
        T.StructField("standalone_query_clean", T.StringType(), True),
        T.StructField("display_question", T.StringType(), True),
        T.StructField("model", T.StringType(), True),
        T.StructField("is_document_rag", T.BooleanType(), True),
        T.StructField("question_length", T.IntegerType(), True),
        T.StructField("answer_length", T.IntegerType(), True),
        T.StructField("standalone_query_length", T.IntegerType(), True),
        T.StructField("risk_level", T.StringType(), True),
        T.StructField("sentiment", T.StringType(), True),
        T.StructField("topic", T.StringType(), True),
        T.StructField("sensitive_flag", T.BooleanType(), True),
        T.StructField("processed_at", T.TimestampType(), True),
        T.StructField("source_layer", T.StringType(), True),
        T.StructField("target_layer", T.StringType(), True),
        T.StructField("is_valid", T.BooleanType(), True),
        T.StructField("date", T.DateType(), True),
        T.StructField("hour", T.IntegerType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create realtime Gold chatbot metrics from Silver streaming files.")
    parser.add_argument("--input-path", default=SILVER_CHAT_PATH)
    parser.add_argument("--output-path", default=GOLD_REALTIME_METRICS_PATH)
    parser.add_argument("--checkpoint-path", default=CHECKPOINT_PATH)
    return parser.parse_args()


def count_if(condition):
    return F.sum(F.when(condition, 1).otherwise(0)).cast("long")


def metrics_for_window(df: DataFrame, duration: str) -> DataFrame:
    grouped = (
        df.withWatermark("timestamp", "2 hours")
        .groupBy(F.window("timestamp", duration), F.col("model"))
        .agg(
            F.count(F.lit(1)).alias("total_messages"),
            F.approx_count_distinct("anonymous_session_id").alias("unique_sessions"),
            count_if(F.col("risk_level") == "high").alias("high_risk_count"),
            count_if(F.col("is_document_rag") == F.lit(True)).alias("rag_messages"),
            count_if(F.col("sentiment") == "positive").alias("positive_count"),
            count_if(F.col("sentiment") == "neutral").alias("neutral_count"),
            count_if(F.col("sentiment") == "negative").alias("negative_count"),
            count_if(F.col("topic") == "harm_intent").alias("harm_intent_count"),
            count_if(F.col("topic") == "self_harm").alias("self_harm_count"),
            count_if(F.col("topic") == "mental_health").alias("mental_health_count"),
            count_if(F.col("topic") == "rag_question").alias("rag_question_count"),
            count_if(F.col("topic") == "general").alias("general_count"),
        )
        .withColumn("window_duration", F.lit(duration))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("date", F.to_date("window_start"))
        .withColumn("hour", F.hour("window_start"))
        .withColumn("rag_rate", F.round(F.col("rag_messages") / F.col("total_messages") * 100, 2))
        .withColumn("processed_at", F.current_timestamp())
        .drop("window")
    )
    return grouped.select(
        "window_duration",
        "window_start",
        "window_end",
        "date",
        "hour",
        "model",
        "total_messages",
        "unique_sessions",
        "high_risk_count",
        "rag_messages",
        "rag_rate",
        "positive_count",
        "neutral_count",
        "negative_count",
        "harm_intent_count",
        "self_harm_count",
        "mental_health_count",
        "rag_question_count",
        "general_count",
        "processed_at",
    )


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("chat-silver-stream-to-gold-realtime").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver = (
        spark.readStream.schema(SILVER_SCHEMA)
        .option("basePath", args.input_path)
        .parquet(args.input_path)
        .where(F.col("timestamp").isNotNull())
    )

    windows = [metrics_for_window(silver, "1 minute"), metrics_for_window(silver, "5 minutes"), metrics_for_window(silver, "1 hour")]
    realtime_metrics = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), windows)

    query = (
        realtime_metrics.writeStream.format("parquet")
        .option("checkpointLocation", args.checkpoint_path)
        .option("path", args.output_path)
        .outputMode("append")
        .partitionBy("date", "window_duration")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
