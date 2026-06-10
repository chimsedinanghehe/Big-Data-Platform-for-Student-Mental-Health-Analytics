"""Chat Silver -> Gold batch job for Dataproc Serverless Spark.

Scope:
- Read only cleaned/anonymized chat records from silver/anonymized_chat.
- Build dashboard-ready aggregate metrics.
- Write only chat Gold tables.
- Never read Bronze and never process survey, knowledge base, vector, embeddings, or RAG chunks.
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import StorageLevel


SILVER_CHAT_PATH = "gs://student-mental-health-lake-nhom1-2026/silver/anonymized_chat/"
PROJECT_ID = "student-mental-health-496205"
GOLD_CHAT_HOURLY_METRICS_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_hourly_metrics/"
GOLD_CHAT_RISK_SUMMARY_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_risk_summary/"
GOLD_CHAT_TOPIC_SUMMARY_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_topic_summary/"
GOLD_CHAT_CONSTRUCT_SUMMARY_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_construct_summary/"
GOLD_CHAT_MODEL_USAGE_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_model_usage/"
GOLD_CHAT_SENTIMENT_SUMMARY_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/sentiment_summary/chat_sentiment_summary/"
WRITE_MODE = "overwrite"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create Gold chatbot dashboard metrics from Silver chat Parquet only.")
    parser.add_argument("--input-path", default=SILVER_CHAT_PATH)
    parser.add_argument("--process-date", help="Filter Silver chat partition date=YYYY-MM-DD for daily backfill/reprocess.")
    parser.add_argument("--hourly-output-path", default=GOLD_CHAT_HOURLY_METRICS_PATH)
    parser.add_argument("--risk-output-path", default=GOLD_CHAT_RISK_SUMMARY_PATH)
    parser.add_argument("--topic-output-path", default=GOLD_CHAT_TOPIC_SUMMARY_PATH)
    parser.add_argument("--construct-output-path", default=GOLD_CHAT_CONSTRUCT_SUMMARY_PATH)
    parser.add_argument("--model-output-path", default=GOLD_CHAT_MODEL_USAGE_PATH)
    parser.add_argument("--sentiment-output-path", default=GOLD_CHAT_SENTIMENT_SUMMARY_PATH)
    parser.add_argument("--gold-max-single-file-rows", type=int, default=100000)
    parser.add_argument("--gold-output-partitions", type=int, default=16)
    parser.add_argument("--write-mode", default=WRITE_MODE, choices=["overwrite", "append", "errorifexists", "ignore"])
    return parser.parse_args()


def print_json_log(payload: dict) -> None:
    print("JOB_JSON_LOG " + json.dumps(payload, default=str, sort_keys=True))


def ensure_column(df: DataFrame, name: str, expression) -> DataFrame:
    if name in df.columns:
        return df
    print(f"WARNING: Silver chat missing column {name}; adding fallback value.")
    return df.withColumn(name, expression)


def normalize_silver(df: DataFrame) -> DataFrame:
    df = ensure_column(df, "timestamp", F.current_timestamp())
    if "date" not in df.columns:
        print("WARNING: Silver chat missing date; deriving date from timestamp.")
        df = df.withColumn("date", F.to_date("timestamp"))
    if "hour" not in df.columns:
        print("WARNING: Silver chat missing hour; deriving hour from timestamp.")
        df = df.withColumn("hour", F.hour("timestamp"))

    fallbacks = {
        "anonymous_session_id": F.lit("unknown").cast("string"),
        "question_clean": F.lit("").cast("string"),
        "standalone_query_clean": F.lit("").cast("string"),
        "model": F.lit("unknown").cast("string"),
        "is_document_rag": F.lit(False).cast("boolean"),
        "question_length": F.lit(0).cast("int"),
        "answer_length": F.lit(0).cast("int"),
        "risk_level": F.lit("unknown").cast("string"),
        "sentiment": F.lit("unknown").cast("string"),
        "topic": F.lit("unknown").cast("string"),
    }
    for name, expression in fallbacks.items():
        df = ensure_column(df, name, expression)

    return (
        df.withColumn("date", F.col("date").cast("date"))
        .withColumn("hour", F.col("hour").cast("int"))
        .withColumn("question_clean", F.coalesce(F.col("question_clean").cast("string"), F.lit("")))
        .withColumn("standalone_query_clean", F.coalesce(F.col("standalone_query_clean").cast("string"), F.lit("")))
        .withColumn("is_document_rag", F.coalesce(F.col("is_document_rag").cast("boolean"), F.lit(False)))
        .withColumn("question_length", F.coalesce(F.col("question_length").cast("double"), F.lit(0.0)))
        .withColumn("answer_length", F.coalesce(F.col("answer_length").cast("double"), F.lit(0.0)))
        .withColumn("risk_level", F.coalesce(F.col("risk_level").cast("string"), F.lit("unknown")))
        .withColumn("sentiment", F.coalesce(F.col("sentiment").cast("string"), F.lit("unknown")))
        .withColumn("topic", F.coalesce(F.col("topic").cast("string"), F.lit("unknown")))
        .withColumn("model", F.coalesce(F.col("model").cast("string"), F.lit("unknown")))
        .where(F.col("date").isNotNull() & F.col("hour").isNotNull())
    )


def count_if(condition):
    return F.sum(F.when(condition, 1).otherwise(0)).cast("long")


def build_hourly_metrics(df: DataFrame) -> DataFrame:
    grouped = (
        df.groupBy("date", "hour")
        .agg(
            F.count(F.lit(1)).alias("total_messages"),
            F.countDistinct("anonymous_session_id").alias("unique_sessions"),
            count_if(F.col("is_document_rag") == F.lit(True)).alias("rag_messages"),
            count_if(F.col("is_document_rag") == F.lit(False)).alias("non_rag_messages"),
            F.avg("question_length").alias("avg_question_length"),
            F.avg("answer_length").alias("avg_answer_length"),
            count_if(F.col("risk_level") == "high").alias("high_risk_count"),
            count_if(F.col("risk_level") == "medium").alias("medium_risk_count"),
            count_if(F.col("risk_level") == "low").alias("low_risk_count"),
            count_if(F.col("sentiment") == "positive").alias("positive_count"),
            count_if(F.col("sentiment") == "neutral").alias("neutral_count"),
            count_if(F.col("sentiment") == "negative").alias("negative_count"),
            count_if(F.col("topic") == "harm_intent").alias("harm_intent_count"),
            count_if(F.col("topic") == "self_harm").alias("self_harm_count"),
            count_if(F.col("topic") == "mental_health").alias("mental_health_count"),
            count_if(F.col("topic") == "rag_question").alias("rag_question_count"),
            count_if(F.col("topic") == "general").alias("general_count"),
        )
        .withColumn("rag_rate", F.round(F.col("rag_messages") / F.col("total_messages") * 100, 2))
        .withColumn("avg_question_length", F.round("avg_question_length", 2))
        .withColumn("avg_answer_length", F.round("avg_answer_length", 2))
        .withColumn("processed_at", F.current_timestamp())
    )
    return grouped.select(
        "date",
        "hour",
        "total_messages",
        "unique_sessions",
        "rag_messages",
        "non_rag_messages",
        "rag_rate",
        "avg_question_length",
        "avg_answer_length",
        "high_risk_count",
        "medium_risk_count",
        "low_risk_count",
        "positive_count",
        "neutral_count",
        "negative_count",
        "harm_intent_count",
        "self_harm_count",
        "mental_health_count",
        "rag_question_count",
        "general_count",
        "processed_at",
    ).orderBy("date", "hour")


def build_distribution(df: DataFrame, column_name: str, value_name: str) -> DataFrame:
    total_window = Window.partitionBy("date")
    return (
        df.groupBy("date", F.col(column_name).cast("string").alias(value_name))
        .agg(F.count(F.lit(1)).alias("count"))
        .withColumn("__date_total", F.sum("count").over(total_window))
        .withColumn("percentage", F.round(F.col("count") / F.col("__date_total") * 100, 2))
        .drop("__date_total")
        .orderBy("date", value_name)
    )


def add_chat_construct(df: DataFrame) -> DataFrame:
    text = F.lower(F.concat_ws(" ", F.col("question_clean"), F.col("standalone_query_clean")))
    construct = (
        F.when(
            (F.col("risk_level") == "high")
            | F.col("topic").isin("harm_intent", "self_harm")
            | text.rlike(r"(kill|murder|suicide|self\s*harm|harm\s+someone|hurt\s+someone|hurt\s+myself|kill\s+myself)"),
            F.lit("Nguy cơ an toàn cấp cao"),
        )
        .when(
            text.rlike(r"(abuse|assault|violence|violent|trauma|stalk|harass|rape|partner|hit|beaten|bully|bị\s*bạo\s*lực|xâm\s*hại)"),
            F.lit("Sang chấn, bạo lực & tổn hại"),
        )
        .when(
            text.rlike(r"(depress|depression|anxiety|panic|hopeless|sad|stress|stressed|mental|lonely|overwhelm|burnout|trầm\s*cảm|lo\s*âu|căng\s*thẳng)"),
            F.lit("Tâm trạng, lo âu & trầm cảm"),
        )
        .when(
            text.rlike(r"(exam|grade|study|school|college|university|class|homework|assignment|deadline|academic|fail|teacher|professor|học|thi|điểm|bài\s*tập)"),
            F.lit("Áp lực học tập & thành tích"),
        )
        .when(
            text.rlike(r"(family|parent|mother|father|home|housing|money|financial|tuition|food|hungry|rent|job|work|gia\s*đình|tài\s*chính|tiền)"),
            F.lit("Gia đình, tài chính & nhu cầu cơ bản"),
        )
        .when(
            text.rlike(r"(friend|relationship|belong|alone|isolat|unsafe|safe|campus|social|peer|breakup|bạn\s*bè|cô\s*đơn|an\s*toàn)"),
            F.lit("Gắn kết xã hội & an toàn môi trường"),
        )
        .when(
            text.rlike(r"(alcohol|drink|drunk|drug|weed|marijuana|vape|smok|cigarette|substance|rượu|thuốc\s*lá|ma\s*túy|chất\s*kích\s*thích)"),
            F.lit("Rượu, thuốc lá & chất kích thích"),
        )
        .when(
            text.rlike(r"(sleep|insomnia|tired|exhausted|exercise|eat|eating|breakfast|rest|ngủ|mệt|vận\s*động|ăn\s*uống)"),
            F.lit("Thiếu ngủ, phục hồi & sinh hoạt"),
        )
        .otherwise(F.lit("Hỗ trợ chung / chưa rõ cụm"))
    )
    return df.withColumn("chat_construct", construct)


def build_construct_summary(df: DataFrame) -> DataFrame:
    with_construct = add_chat_construct(df)
    total_window = Window.partitionBy("date")
    return (
        with_construct.groupBy("date", "chat_construct")
        .agg(
            F.count(F.lit(1)).alias("count"),
            count_if(F.col("risk_level") == "high").alias("high_risk_count"),
            count_if(F.col("sentiment") == "negative").alias("negative_count"),
            count_if(F.col("is_document_rag") == F.lit(True)).alias("rag_messages"),
            F.countDistinct("anonymous_session_id").alias("unique_sessions"),
            F.avg("question_length").alias("avg_question_length"),
        )
        .withColumn("__date_total", F.sum("count").over(total_window))
        .withColumn("percentage", F.round(F.col("count") / F.col("__date_total") * 100, 2))
        .withColumn("high_risk_rate", F.round(F.col("high_risk_count") / F.col("count") * 100, 2))
        .withColumn("negative_rate", F.round(F.col("negative_count") / F.col("count") * 100, 2))
        .withColumn("rag_rate", F.round(F.col("rag_messages") / F.col("count") * 100, 2))
        .withColumn("avg_question_length", F.round("avg_question_length", 2))
        .withColumn("processed_at", F.current_timestamp())
        .drop("__date_total")
        .orderBy("date", F.desc("count"), "chat_construct")
    )


def output_table(
    df: DataFrame,
    path: str,
    table_name: str,
    write_mode: str,
    max_single_file_rows: int,
    output_partitions: int,
) -> int:
    print(f"\nGOLD TABLE: {table_name}")
    df.printSchema()
    df.show(20, truncate=False)
    rows = df.count()
    print(f"{table_name} rows: {rows}")
    print(f"Writing {table_name} to {path} mode={write_mode}")
    if rows <= max_single_file_rows:
        # Gold aggregates are small dashboard-ready tables, so one file is convenient for simple loaders.
        # Do not use coalesce(1) for Silver or large record-level datasets.
        df.coalesce(1).write.mode(write_mode).parquet(path)
    else:
        partition_columns = [column for column in ["date"] if column in df.columns]
        writer_df = df.repartition(max(1, output_partitions), *partition_columns) if partition_columns else df.repartition(max(1, output_partitions))
        if partition_columns:
            writer_df.write.mode(write_mode).partitionBy(*partition_columns).parquet(path)
        else:
            writer_df.write.mode(write_mode).parquet(path)
    return rows


def main() -> None:
    args = parse_args()
    start_time = time.time()
    spark = (
        SparkSession.builder.appName("chat-silver-to-gold-dashboard-metrics")
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.default.parallelism", "24")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Reading Silver chat only from: {args.input_path}")
        silver = normalize_silver(spark.read.parquet(args.input_path))
        if args.process_date:
            print(f"Filtering Silver chat for process date: {args.process_date}")
            silver = silver.where(F.col("date") == F.to_date(F.lit(args.process_date)))
        # Reused by five Gold builders; MEMORY_AND_DISK avoids repeated Parquet scans without relying only on executor memory.
        silver = silver.persist(StorageLevel.MEMORY_AND_DISK)
        input_rows = silver.count()
        print(f"Silver rows available for Gold chat metrics: {input_rows}")
        silver.printSchema()

        tables: Dict[str, DataFrame] = {
            "chat_hourly_metrics": build_hourly_metrics(silver),
            "chat_risk_summary": build_distribution(silver, "risk_level", "risk_level"),
            "chat_topic_summary": build_distribution(silver, "topic", "topic"),
            "chat_construct_summary": build_construct_summary(silver),
            "chat_model_usage": build_distribution(silver, "model", "model"),
            "chat_sentiment_summary": build_distribution(silver, "sentiment", "sentiment"),
        }

        output_rows = {
            "chat_hourly_metrics": output_table(
                tables["chat_hourly_metrics"],
                args.hourly_output_path,
                "chat_hourly_metrics",
                args.write_mode,
                args.gold_max_single_file_rows,
                args.gold_output_partitions,
            ),
            "chat_risk_summary": output_table(
                tables["chat_risk_summary"],
                args.risk_output_path,
                "chat_risk_summary",
                args.write_mode,
                args.gold_max_single_file_rows,
                args.gold_output_partitions,
            ),
            "chat_topic_summary": output_table(
                tables["chat_topic_summary"],
                args.topic_output_path,
                "chat_topic_summary",
                args.write_mode,
                args.gold_max_single_file_rows,
                args.gold_output_partitions,
            ),
            "chat_construct_summary": output_table(
                tables["chat_construct_summary"],
                args.construct_output_path,
                "chat_construct_summary",
                args.write_mode,
                args.gold_max_single_file_rows,
                args.gold_output_partitions,
            ),
            "chat_model_usage": output_table(
                tables["chat_model_usage"],
                args.model_output_path,
                "chat_model_usage",
                args.write_mode,
                args.gold_max_single_file_rows,
                args.gold_output_partitions,
            ),
            "chat_sentiment_summary": output_table(
                tables["chat_sentiment_summary"],
                args.sentiment_output_path,
                "chat_sentiment_summary",
                args.write_mode,
                args.gold_max_single_file_rows,
                args.gold_output_partitions,
            ),
        }
        print_json_log(
            {
                "job_name": "chat_silver_to_gold",
                "project_id": PROJECT_ID,
                "input_path": args.input_path,
                "process_date": args.process_date,
                "write_mode": args.write_mode,
                "input_rows": input_rows,
                "output_rows": output_rows,
                "output_paths": {
                    "chat_hourly_metrics": args.hourly_output_path,
                    "chat_risk_summary": args.risk_output_path,
                    "chat_topic_summary": args.topic_output_path,
                    "chat_construct_summary": args.construct_output_path,
                    "chat_model_usage": args.model_output_path,
                    "chat_sentiment_summary": args.sentiment_output_path,
                },
                "output_success": True,
                "duration_seconds": round(time.time() - start_time, 2),
                "status": "success",
            }
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
