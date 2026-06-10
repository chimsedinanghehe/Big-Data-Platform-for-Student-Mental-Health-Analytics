"""Chat Silver -> Gold batch job for Dataproc Serverless Spark.

Scope:
- Read only cleaned/anonymized chat records from Silver.
- Build dashboard-ready aggregate metrics.
- Preserve audience_group so dashboard pages can show Tổng quan, Học sinh, and
  Sinh viên views from Gold without reading Bronze.
- Keep production fast: no count/show/printSchema unless explicitly requested.
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Dict, Iterable, List

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


PROJECT_ID = "student-mental-health-496205"
SILVER_CHAT_PATH = "gs://student-mental-health-lake-nhom1-2026/silver/anonymized_chat/"
GOLD_CHAT_HOURLY_METRICS_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_hourly_metrics/"
GOLD_CHAT_RISK_SUMMARY_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_risk_summary/"
GOLD_CHAT_TOPIC_SUMMARY_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_topic_summary/"
GOLD_CHAT_CONSTRUCT_SUMMARY_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_construct_summary/"
GOLD_CHAT_MODEL_USAGE_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_model_usage/"
GOLD_CHAT_SENTIMENT_SUMMARY_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/sentiment_summary/chat_sentiment_summary/"
WRITE_MODE = "overwrite"
NOT_COUNTED = "not_counted_for_speed"
SUMMARY_VALUE_LABELS = {
    "risk_level": {
        "low": "Thấp",
        "medium": "Trung bình",
        "high": "Cao",
        "unknown": "Không rõ",
    },
    "sentiment": {
        "positive": "Tích cực",
        "neutral": "Trung tính",
        "negative": "Tiêu cực",
        "unknown": "Không rõ",
    },
    "topic": {
        "rag_question": "Hỏi tài liệu / hỗ trợ học tập",
        "harm_intent": "Nguy cơ gây hại người khác",
        "self_harm": "Nguy cơ tự gây hại",
        "mental_health": "Sức khỏe tinh thần",
        "general": "Hỗ trợ chung",
        "unknown": "Chưa xác định chủ đề",
    },
}

CANONICAL_TABLES = [
    "hourly",
    "risk",
    "topic",
    "construct",
    "model",
    "sentiment",
]
TABLE_ALIASES = {
    "all": CANONICAL_TABLES,
    "core": CANONICAL_TABLES,
    "hourly": ["hourly"],
    "chat_hourly_metrics": ["hourly"],
    "risk": ["risk"],
    "chat_risk_summary": ["risk"],
    "topic": ["topic"],
    "chat_topic_summary": ["topic"],
    "construct": ["construct"],
    "chat_construct_summary": ["construct"],
    "model": ["model"],
    "chat_model_usage": ["model"],
    "sentiment": ["sentiment"],
    "chat_sentiment_summary": ["sentiment"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create Gold chatbot dashboard metrics from Silver chat Parquet only.")
    parser.add_argument("--input-path", default=SILVER_CHAT_PATH)
    parser.add_argument("--process-date", help="Filter Silver chat partition date=YYYY-MM-DD for daily backfill/reprocess.")
    parser.add_argument("--tables", default="all", help="Comma-separated tables: hourly,risk,topic,construct,model,sentiment.")
    parser.add_argument("--hourly-output-path", default=GOLD_CHAT_HOURLY_METRICS_PATH)
    parser.add_argument("--risk-output-path", default=GOLD_CHAT_RISK_SUMMARY_PATH)
    parser.add_argument("--topic-output-path", default=GOLD_CHAT_TOPIC_SUMMARY_PATH)
    parser.add_argument("--construct-output-path", default=GOLD_CHAT_CONSTRUCT_SUMMARY_PATH)
    parser.add_argument("--model-output-path", default=GOLD_CHAT_MODEL_USAGE_PATH)
    parser.add_argument("--sentiment-output-path", default=GOLD_CHAT_SENTIMENT_SUMMARY_PATH)
    parser.add_argument("--gold-output-partitions", type=int, default=4)
    parser.add_argument("--spark-parallelism", type=int, default=8)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--write-mode", default=WRITE_MODE, choices=["overwrite", "append", "errorifexists", "ignore"])
    parser.add_argument("--enable-quality-report", action="store_true")
    parser.add_argument("--enable-output-verify", action="store_true")
    parser.add_argument("--fast-mode", action="store_true", help="Kept for orchestration clarity; production defaults are already fast.")
    parser.add_argument("--run-id")
    parser.add_argument("--versioned-output", action="store_true")
    parser.add_argument("--partition-gold-by-date", action="store_true")
    return parser.parse_args()


def parse_tables(raw: str) -> List[str]:
    requested: List[str] = []
    for token in (raw or "all").split(","):
        key = token.strip().lower()
        if not key:
            continue
        if key not in TABLE_ALIASES:
            raise ValueError(f"Unknown chat Gold table: {token}")
        for table in TABLE_ALIASES[key]:
            if table not in requested:
                requested.append(table)
    return requested or CANONICAL_TABLES.copy()


def print_json_log(payload: dict) -> None:
    print("JOB_JSON_LOG " + json.dumps(payload, default=str, sort_keys=True))


def effective_path(base_path: str, run_id: str | None, versioned_output: bool) -> str:
    if versioned_output:
        if not run_id:
            raise ValueError("--run-id is required when --versioned-output is enabled")
        return f"{base_path.rstrip('/')}/run_id={run_id}/"
    return base_path


def ensure_column(df: DataFrame, name: str, expression) -> DataFrame:
    if name in df.columns:
        return df
    print(f"WARNING: Silver chat missing column {name}; adding fallback value.")
    return df.withColumn(name, expression)


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
        "user_id_hash": F.lit(None).cast("string"),
        "user_age": F.lit(None).cast("int"),
        "user_gender": F.lit(None).cast("string"),
        "learner_type": F.lit(None).cast("string"),
        "grade": F.lit(None).cast("string"),
        "class_level": F.lit(None).cast("string"),
        "user_group": F.lit(None).cast("string"),
        "audience_group": F.lit(None).cast("string"),
        "survey_type": F.lit(None).cast("string"),
        "survey_completed": F.lit(False).cast("boolean"),
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
        .withColumn("user_age", F.col("user_age").cast("int"))
        .withColumn("user_gender", F.col("user_gender").cast("string"))
        .withColumn("learner_type", F.col("learner_type").cast("string"))
        .withColumn("grade", F.col("grade").cast("string"))
        .withColumn("class_level", F.coalesce(F.col("class_level").cast("string"), F.col("grade").cast("string"), F.col("learner_type").cast("string")))
        .withColumn("audience_group", audience_group_expression())
        .withColumn("user_group", F.coalesce(F.col("user_group").cast("string"), F.col("audience_group")))
        .withColumn("survey_type", F.coalesce(F.col("survey_type").cast("string"), F.col("audience_group")))
        .withColumn("survey_completed", F.coalesce(F.col("survey_completed").cast("boolean"), F.lit(False)))
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


def safe_pct(numerator, denominator):
    return F.when(denominator > 0, F.round(numerator / denominator * 100, 2)).otherwise(F.lit(0.0))


def build_hourly_metrics(df: DataFrame) -> DataFrame:
    grouped = (
        df.groupBy("date", "audience_group", "hour")
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
        .withColumn("rag_rate", safe_pct(F.col("rag_messages"), F.col("total_messages")))
        .withColumn("avg_question_length", F.round("avg_question_length", 2))
        .withColumn("avg_answer_length", F.round("avg_answer_length", 2))
        .withColumn("processed_at", F.current_timestamp())
    )
    return grouped.select(
        "date",
        "audience_group",
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
    )


def build_distribution(df: DataFrame, column_name: str, value_name: str) -> DataFrame:
    total_window = Window.partitionBy("date", "audience_group")
    result = (
        df.groupBy("date", "audience_group", F.col(column_name).cast("string").alias(value_name))
        .agg(F.count(F.lit(1)).alias("count"))
        .withColumn("__group_total", F.sum("count").over(total_window))
        .withColumn("percentage", safe_pct(F.col("count"), F.col("__group_total")))
        .drop("__group_total")
    )
    labels = SUMMARY_VALUE_LABELS.get(column_name)
    return result.replace(labels, subset=[value_name]) if labels else result


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
            text.rlike(r"(abuse|assault|violence|violent|trauma|stalk|harass|rape|partner|hit|beaten|bully|bị\s*bạo\s*lực|xâm\s*hại|bắt\s*nạt|quấy\s*rối|theo\s*dõi)"),
            F.lit("Sang chấn, bạo lực và tổn hại"),
        )
        .when(
            text.rlike(r"(depress|depression|anxiety|panic|hopeless|sad|stress|stressed|mental|lonely|overwhelm|burnout|trầm\s*cảm|lo\s*âu|căng\s*thẳng|buồn|tuyệt\s*vọng|cô\s*đơn|kiệt\s*sức)"),
            F.lit("Tâm trạng, lo âu và trầm cảm"),
        )
        .when(
            text.rlike(r"(exam|grade|study|school|college|university|class|homework|assignment|deadline|academic|fail|teacher|professor|học|thi|điểm|bài\s*tập|hạn\s*nộp|trượt\s*môn)"),
            F.lit("Áp lực học tập và thành tích"),
        )
        .when(
            text.rlike(r"(family|parent|mother|father|home|housing|money|financial|tuition|food|hungry|rent|job|work|gia\s*đình|tài\s*chính|tiền|học\s*phí|nhà\s*ở|đói|việc\s*làm)"),
            F.lit("Gia đình, tài chính và nhu cầu cơ bản"),
        )
        .when(
            text.rlike(r"(friend|relationship|belong|alone|isolat|unsafe|safe|campus|social|peer|breakup|bạn\s*bè|cô\s*đơn|an\s*toàn|chia\s*tay|mối\s*quan\s*hệ|không\s*thuộc\s*về)"),
            F.lit("Gắn kết xã hội và an toàn môi trường"),
        )
        .when(
            text.rlike(r"(alcohol|drink|drunk|drug|weed|marijuana|vape|smok|cigarette|substance|rượu|thuốc\s*lá|ma\s*túy|chất\s*kích\s*thích)"),
            F.lit("Rượu, thuốc lá và chất kích thích"),
        )
        .when(
            text.rlike(r"(sleep|insomnia|tired|exhausted|exercise|eat|eating|breakfast|rest|ngủ|mệt|vận\s*động|ăn\s*uống|mất\s*ngủ|kiệt\s*sức|nghỉ\s*ngơi)"),
            F.lit("Thiếu ngủ, phục hồi và sinh hoạt"),
        )
        .otherwise(F.lit("Hỗ trợ chung / chưa rõ cụm"))
    )
    return df.withColumn("chat_construct", construct)


def build_construct_summary(df: DataFrame) -> DataFrame:
    with_construct = add_chat_construct(df)
    total_window = Window.partitionBy("date", "audience_group")
    return (
        with_construct.groupBy("date", "audience_group", "chat_construct")
        .agg(
            F.count(F.lit(1)).alias("count"),
            count_if(F.col("risk_level") == "high").alias("high_risk_count"),
            count_if(F.col("sentiment") == "negative").alias("negative_count"),
            count_if(F.col("is_document_rag") == F.lit(True)).alias("rag_messages"),
            F.countDistinct("anonymous_session_id").alias("unique_sessions"),
            F.avg("question_length").alias("avg_question_length"),
        )
        .withColumn("__group_total", F.sum("count").over(total_window))
        .withColumn("percentage", safe_pct(F.col("count"), F.col("__group_total")))
        .withColumn("high_risk_rate", safe_pct(F.col("high_risk_count"), F.col("count")))
        .withColumn("negative_rate", safe_pct(F.col("negative_count"), F.col("count")))
        .withColumn("rag_rate", safe_pct(F.col("rag_messages"), F.col("count")))
        .withColumn("avg_question_length", F.round("avg_question_length", 2))
        .withColumn("processed_at", F.current_timestamp())
        .drop("__group_total")
    )


def output_table(
    df: DataFrame,
    path: str,
    table_name: str,
    write_mode: str,
    output_partitions: int,
    partition_gold_by_date: bool,
    enable_output_verify: bool,
) -> int | str:
    print(f"Writing {table_name} to {path} mode={write_mode}")
    rows: int | str = NOT_COUNTED
    if enable_output_verify:
        rows = df.count()

    if partition_gold_by_date and "date" in df.columns:
        writer_df = df.repartition(max(1, output_partitions), "date")
        writer_df.write.mode(write_mode).partitionBy("date").parquet(path)
    else:
        df.coalesce(1).write.mode(write_mode).parquet(path)
    return rows


def print_quality_report(silver: DataFrame) -> None:
    print("\nDATA QUALITY LOG: CHAT SILVER -> GOLD")
    silver.printSchema()
    for column_name in ["date", "audience_group", "risk_level", "sentiment", "topic", "model"]:
        if column_name in silver.columns:
            silver.groupBy(column_name).count().orderBy(column_name).show(100, truncate=False)


def build_table_map(silver: DataFrame, requested: Iterable[str]) -> Dict[str, DataFrame]:
    builders = {
        "hourly": lambda: build_hourly_metrics(silver),
        "risk": lambda: build_distribution(silver, "risk_level", "risk_level"),
        "topic": lambda: build_distribution(silver, "topic", "topic"),
        "construct": lambda: build_construct_summary(silver),
        "model": lambda: build_distribution(silver, "model", "model"),
        "sentiment": lambda: build_distribution(silver, "sentiment", "sentiment"),
    }
    return {table: builders[table]() for table in requested}


def table_output_paths(args: argparse.Namespace) -> Dict[str, str]:
    return {
        "hourly": effective_path(args.hourly_output_path, args.run_id, args.versioned_output),
        "risk": effective_path(args.risk_output_path, args.run_id, args.versioned_output),
        "topic": effective_path(args.topic_output_path, args.run_id, args.versioned_output),
        "construct": effective_path(args.construct_output_path, args.run_id, args.versioned_output),
        "model": effective_path(args.model_output_path, args.run_id, args.versioned_output),
        "sentiment": effective_path(args.sentiment_output_path, args.run_id, args.versioned_output),
    }


def main() -> None:
    args = parse_args()
    requested_tables = parse_tables(args.tables)
    skipped_tables = [table for table in CANONICAL_TABLES if table not in requested_tables]
    start_time = time.time()
    stage_times: Dict[str, float] = {}
    spark = (
        SparkSession.builder.appName("chat-silver-to-gold-dashboard-metrics")
        .config("spark.sql.shuffle.partitions", str(max(1, args.shuffle_partitions)))
        .config("spark.default.parallelism", str(max(1, args.spark_parallelism)))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    input_rows: int | str = NOT_COUNTED
    output_rows: Dict[str, int | str] = {}
    persisted = False

    try:
        read_start = time.time()
        print(f"Reading Silver chat only from: {args.input_path}")
        # The Silver root can contain a legacy Structured Streaming
        # _spark_metadata log. Reading the root would trust that stale log and
        # ignore newer batch-written partitions. An explicit data-file glob
        # bypasses the log while basePath retains date/hour partition columns.
        silver_file_glob = f"{args.input_path.rstrip('/')}/date=*/hour=*/*.parquet"
        print(f"Reading physical Silver Parquet files from: {silver_file_glob}")
        silver = normalize_silver(
            spark.read.option("basePath", args.input_path)
            .option("mergeSchema", "true")
            .parquet(silver_file_glob)
        )
        if args.process_date:
            print(f"Filtering Silver chat for process date: {args.process_date}")
            silver = silver.where(F.col("date") == F.to_date(F.lit(args.process_date)))
        stage_times["read_silver_seconds"] = round(time.time() - read_start, 2)

        if args.enable_quality_report:
            print_quality_report(silver)
        if args.enable_output_verify:
            input_rows = silver.count()

        if len(requested_tables) > 1:
            silver = silver.persist(StorageLevel.MEMORY_AND_DISK)
            persisted = True

        paths = table_output_paths(args)
        tables = build_table_map(silver, requested_tables)
        tables_written: List[str] = []

        for table_name in requested_tables:
            table_start = time.time()
            output_rows[table_name] = output_table(
                tables[table_name],
                paths[table_name],
                table_name,
                args.write_mode,
                args.gold_output_partitions,
                args.partition_gold_by_date,
                args.enable_output_verify,
            )
            stage_times[f"{table_name}_seconds"] = round(time.time() - table_start, 2)
            tables_written.append(table_name)

        print_json_log(
            {
                "job_name": "chat_silver_to_gold",
                "project_id": PROJECT_ID,
                "input_path": args.input_path,
                "process_date": args.process_date,
                "write_mode": args.write_mode,
                "run_id": args.run_id,
                "versioned_output": args.versioned_output,
                "partition_gold_by_date": args.partition_gold_by_date,
                "tables_requested": requested_tables,
                "tables_written": tables_written,
                "skipped_tables": skipped_tables,
                "input_rows": input_rows,
                "output_rows": output_rows,
                "output_paths": {table: paths[table] for table in requested_tables},
                "partition_config": {
                    "gold_output_partitions": max(1, args.gold_output_partitions),
                    "spark_parallelism": max(1, args.spark_parallelism),
                    "shuffle_partitions": max(1, args.shuffle_partitions),
                },
                "count_enabled": args.enable_output_verify,
                "quality_report_enabled": args.enable_quality_report,
                "stage_durations": stage_times,
                "total_duration_seconds": round(time.time() - start_time, 2),
                "status": "success",
            }
        )
    except Exception:
        print_json_log(
            {
                "job_name": "chat_silver_to_gold",
                "project_id": PROJECT_ID,
                "input_path": args.input_path,
                "process_date": args.process_date,
                "tables_requested": requested_tables,
                "stage_durations": stage_times,
                "total_duration_seconds": round(time.time() - start_time, 2),
                "status": "failed",
            }
        )
        raise
    finally:
        if persisted:
            silver.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
