"""Bronze -> Silver survey cleaning job for Dataproc Serverless Spark.

Scope:
- Read the two reduced survey CSV files under bronze/survey_standardized.
- Optionally read the app survey snapshot under bronze/app_survey_snapshot/survey_all.parquet.
- Clean, normalize, validate, mask identifiers, and deduplicate records.
- Write record-level Parquet only to silver/survey_cleaned.
- Do not create dashboard metrics and do not write Gold.
"""

from __future__ import annotations

import argparse
import json
import re
import time
import unicodedata
from functools import reduce
from typing import Dict, Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import StorageLevel


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
BRONZE_SURVEY_PATH = "gs://student-mental-health-lake-nhom1-2026/bronze/survey_standardized/"
APP_SURVEY_SNAPSHOT_PATH = "gs://student-mental-health-lake-nhom1-2026/bronze/app_survey_snapshot/survey_all.parquet"
SILVER_SURVEY_PATH = "gs://student-mental-health-lake-nhom1-2026/silver/survey_cleaned/"
INVALID_SURVEY_PATH = "gs://student-mental-health-lake-nhom1-2026/silver/survey_cleaned_invalid/"
WRITE_MODE = "overwrite"

INPUT_SURVEY_FILES = [
    "school_survey_standardized.csv",
    "university_survey_standardized.csv",
]

MISSING_STRINGS = {"", "na", "n/a", "null", "none", "unknown", "-", "--"}
TEXT_YES_STRINGS = {"yes", "y"}
TEXT_NO_STRINGS = {"no", "n"}
TEXT_TRUE_STRINGS = {"true", "t"}
TEXT_FALSE_STRINGS = {"false", "f"}
BOOLEAN_YES_STRINGS = {"yes", "y", "1"}
BOOLEAN_NO_STRINGS = {"no", "n", "0"}
BOOLEAN_TRUE_STRINGS = {"true", "t"}
BOOLEAN_FALSE_STRINGS = {"false", "f"}

TEXT_TOKENS = ("text", "free_text", "comment", "answer", "message", "description", "feedback")
BOOLEAN_NAME_TOKENS = ("yes_no", "true_false", "is_", "has_", "have_", "ever_", "currently_")
DASHBOARD_SCHOOL_QNUMS = sorted(
    {
        1,
        2,
        3,
        8,
        9,
        10,
        11,
        12,
        13,
        *range(14, 19),
        *range(19, 23),
        23,
        24,
        25,
        26,
        27,
        28,
        29,
        30,
        84,
        88,
        *range(31, 41),
        *range(41, 56),
        92,
        93,
        *range(56, 66),
        94,
        *range(68, 81),
        81,
        82,
        83,
        85,
        86,
        95,
        96,
        97,
        98,
        *range(89, 108),
    }
)
DASHBOARD_SCHOOL_Q_COLUMNS = {f"q{qnum}" for qnum in DASHBOARD_SCHOOL_QNUMS}
SCHOOL_CLUSTER_QNUMS = DASHBOARD_SCHOOL_QNUMS
DASHBOARD_HMS_COLUMNS = {
    "responseid",
    "schoolnum",
    "inst_hmsyear",
    "age",
    "sex_birth",
    "gender_male",
    "gender_female",
    "gender_queer",
    "gender_nonbin",
    "gender_trans",
    "gender_transm",
    "gender_transf",
    "yr_sch",
    "deprawsc",
    "anx_score",
    "dep_maj",
    "dep_any",
    "anx_any",
    "dep_or_anx",
    "sui_idea",
    "sui_plan",
    "sui_att",
    "housing_worry",
    "housing",
    "housing1",
    "food_worry",
    "fincur",
    "finpast",
    "afford_school",
    "afford_food",
    "afford_transp",
    "afford_hc",
    "afford_books",
    "afford_house",
    "pay_worry",
    "pay_worry1",
    "pay_worry2",
    "pay_worry3",
    "aca_impa",
    "acad_imp",
    "aca_stress",
    "stress1",
    "stress2",
    "stress3",
    "stress4",
    "compet_sch",
    "compet1",
    "grade_curv",
    "imposter_1",
    "imposter_2",
    "imposter_3",
    "imposter_4",
    "imposter_5",
    "failed",
    "adjust_aca_1",
    "adjust_aca_2",
    "time_manage",
    "doubt_school_1",
    "fam_support_aca",
    "prof_support_aca",
    "belong1",
    "belong2",
    "belong8",
    "belong9",
    "belong",
    "discrim_race",
    "discrim_culture",
    "discrim_gender",
    "discrim_sexual",
    "discrim_other",
    "discrim",
    "safe_on_day",
    "safe_on_night",
    "safe_off_day",
    "safe_off_night",
    "safe_on",
    "safe_off",
    "hostcli_distress",
    "hostcli",
    "abuse_life",
    "abuse_recent",
    "stalk_exp",
    "stalk_life",
    "stalk_recent",
    "assault_life",
    "assault_recent",
    "assault_sex",
    "assault_sex_y",
    "sa_exp",
    "ipv_1",
    "ipv_2",
    "ipv_3",
    "ipv_4",
    "ipv_5",
    "partner_phys",
    "partner_insult",
    "partner_threat",
    "partner_curse",
    "alc_any",
    "binge_fr",
    "sub_any",
    "sub_cig",
    "smok_freq",
    "smok_vape",
    "drug_mar",
    "mar_freq",
    "sleep_wknight",
    "sleep_wkend",
    "exerc",
    "exerc_range5",
    "exerc_range4",
}
SENSITIVE_IDENTIFIERS = {
    "id",
    "responseid",
    "response_id",
    "record",
    "orig_rec",
    "student_id",
    "user_id",
    "email",
    "name",
    "full_name",
    "phone",
    "phone_number",
    "telephone",
}

NUMERIC_NAME_PATTERN = re.compile(
    r"^(age|grade|q\d+|qn[a-z0-9_]*|.*score.*|.*scale.*|.*rating.*|.*_count|.*_total)$",
    re.IGNORECASE,
)
DATE_NAME_PATTERN = re.compile(
    r"(^date$|date$|timestamp|created_at|submitted_at|submission_time|start_time|end_time)",
    re.IGNORECASE,
)
DEDUP_METADATA_COLUMNS = {
    "source_file",
    "source_group",
    "source_dataset",
    "date",
    "year",
    "month",
    "day",
    "ingestion_date",
    "processed_at",
    "source_layer",
    "target_layer",
    "is_valid",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean Bronze survey CSV files into record-level Silver Parquet.")
    parser.add_argument("--input-path", default=BRONZE_SURVEY_PATH)
    parser.add_argument("--app-snapshot-path", default=APP_SURVEY_SNAPSHOT_PATH)
    parser.add_argument(
        "--skip-app-snapshot",
        dest="include_app_snapshot",
        action="store_false",
        help="Do not merge bronze/app_survey_snapshot/survey_all.parquet into Silver.",
    )
    parser.set_defaults(include_app_snapshot=True)
    parser.add_argument("--output-path", default=SILVER_SURVEY_PATH)
    parser.add_argument("--invalid-output-path", default=INVALID_SURVEY_PATH)
    parser.add_argument("--output-partitions", type=int, default=4)
    parser.add_argument("--spark-parallelism", type=int, default=8)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--enable-quality-report", action="store_true", help="Run expensive schema/null/sample/group quality report.")
    parser.add_argument("--enable-output-verify", action="store_true", help="Read output from GCS and count rows after write.")
    parser.add_argument("--write-invalid-records", action="store_true", help="Write invalid/empty survey payload rows without a pre-count.")
    parser.add_argument("--fast-mode", action="store_true", default=True, help="Production mode with minimal Spark actions. Enabled by default.")
    parser.add_argument("--debug-mode", dest="fast_mode", action="store_false", help="Disable fast-mode shortcuts.")
    parser.add_argument("--run-id", help="Optional run id for versioned Silver output.")
    parser.add_argument("--versioned-output", action="store_true", help="Write to output_path/run_id=<run_id>/ instead of the root path.")
    parser.add_argument("--write-mode", default=WRITE_MODE, choices=["overwrite", "append", "errorifexists", "ignore"])
    return parser.parse_args()


def print_json_log(payload: dict) -> None:
    print("JOB_JSON_LOG " + json.dumps(payload, default=str, sort_keys=True))


def default_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def versioned_path(path: str, run_id: str, enabled: bool) -> str:
    if not enabled:
        return path
    return f"{path.rstrip('/')}/run_id={run_id}/"


def effective_write_mode(write_mode: str, versioned_output: bool) -> str:
    if versioned_output and write_mode == "overwrite":
        return "errorifexists"
    return write_mode


def normalize_column_name(name: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", ascii_name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "unnamed_column"


def normalize_columns(df: DataFrame) -> DataFrame:
    used: Dict[str, int] = {}
    result = df
    for original in df.columns:
        base = normalize_column_name(original)
        suffix = used.get(base, 0)
        used[base] = suffix + 1
        new_name = base if suffix == 0 else f"{base}_{suffix + 1}"
        if original != new_name:
            result = result.withColumnRenamed(original, new_name)
    return result


def expected_file_paths(spark: SparkSession, input_path: str) -> Dict[str, str]:
    base = input_path.rstrip("/")
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    filesystem = jvm.org.apache.hadoop.fs.Path(input_path).getFileSystem(conf)

    resolved: Dict[str, str] = {}
    missing: List[str] = []
    for filename in INPUT_SURVEY_FILES:
        path = f"{base}/{filename}"
        if filesystem.exists(jvm.org.apache.hadoop.fs.Path(path)):
            resolved[filename] = path
        else:
            missing.append(path)

    if missing:
        discovered = {}
        for status in filesystem.listStatus(jvm.org.apache.hadoop.fs.Path(input_path)):
            path = status.getPath()
            name = path.getName()
            if name.lower().endswith(".csv"):
                discovered[name] = path.toString()
        if discovered:
            print("Using discovered Bronze survey CSV files instead of legacy fixed filenames.")
            for filename, path in sorted(discovered.items()):
                print(f"  - {filename}: {path}")
            return dict(sorted(discovered.items()))
        message = "\n".join(f"  - {path}" for path in missing)
        raise FileNotFoundError(f"Missing required survey Bronze CSV file(s):\n{message}")

    print("Survey Bronze input files:")
    for filename, path in resolved.items():
        print(f"  - {filename}: {path}")
    return resolved


def gcs_path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(conf)
    return bool(filesystem.exists(hadoop_path))


def source_group_from_name(filename: str):
    lower_name = filename.lower()
    if "school" in lower_name:
        return F.lit("school")
    if "university" in lower_name:
        return F.lit("university")
    return F.lit("unknown")


def read_one_csv(spark: SparkSession, filename: str, path: str) -> DataFrame:
    frame = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .option("multiLine", False)
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )
    frame = normalize_columns(frame)
    kept_columns = [column for column in frame.columns if should_keep_survey_column(column)]
    if kept_columns:
        frame = frame.select(*kept_columns)
    return (
        frame.withColumn("source_file", F.lit(filename))
        .withColumn("source_group", source_group_from_name(filename))
        .withColumn("source_dataset", F.regexp_replace(F.lit(filename), r"\.csv$", ""))
    )


def read_bronze_survey_csvs(spark: SparkSession, input_path: str) -> DataFrame:
    frames = [read_one_csv(spark, filename, path) for filename, path in expected_file_paths(spark, input_path).items()]
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    return combined


def read_app_survey_snapshot(spark: SparkSession, snapshot_path: str) -> Optional[DataFrame]:
    if not snapshot_path or not gcs_path_exists(spark, snapshot_path):
        print(f"App survey snapshot not found, skipping: {snapshot_path}")
        return None

    frame = normalize_columns(spark.read.parquet(snapshot_path))
    kept_columns = [column for column in frame.columns if should_keep_survey_column(column)]
    if kept_columns:
        frame = frame.select(*kept_columns)

    frame = frame.select(*[F.col(column).cast("string").alias(column) for column in frame.columns])
    frame = frame.withColumn("source_file", F.lit("app_survey_snapshot.parquet"))
    if "source_group" in frame.columns:
        frame = frame.withColumn("source_group", F.coalesce(F.col("source_group"), F.col("survey_type"), F.lit("unknown")))
    elif "survey_type" in frame.columns:
        frame = frame.withColumn("source_group", F.coalesce(F.col("survey_type"), F.lit("unknown")))
    else:
        frame = frame.withColumn("source_group", F.lit("unknown"))

    if "source_dataset" in frame.columns:
        frame = frame.withColumn("source_dataset", F.coalesce(F.col("source_dataset"), F.lit("app_survey_snapshot")))
    else:
        frame = frame.withColumn("source_dataset", F.lit("app_survey_snapshot"))
    print(f"Including app survey snapshot: {snapshot_path}")
    return frame


def read_bronze_survey_inputs(
    spark: SparkSession,
    input_path: str,
    app_snapshot_path: str,
    include_app_snapshot: bool,
) -> DataFrame:
    frames = [read_bronze_survey_csvs(spark, input_path)]
    if include_app_snapshot:
        app_snapshot = read_app_survey_snapshot(spark, app_snapshot_path)
        if app_snapshot is not None:
            frames.append(app_snapshot)
    else:
        print("Skipping app survey snapshot because --skip-app-snapshot was set.")

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    return combined


def is_text_column(name: str) -> bool:
    lowered = name.lower()
    return any(token in lowered for token in TEXT_TOKENS)


def is_sensitive_identifier(name: str) -> bool:
    lowered = name.lower()
    return lowered in SENSITIVE_IDENTIFIERS or lowered.endswith("_id") or lowered.endswith("_email")


def should_keep_survey_column(name: str) -> bool:
    lowered = name.lower()
    if is_sensitive_identifier(lowered) or DATE_NAME_PATTERN.search(lowered):
        return True
    if lowered in DASHBOARD_SCHOOL_Q_COLUMNS or lowered in DASHBOARD_HMS_COLUMNS:
        return True
    if lowered in {
        "gender",
        "sex",
        "age",
        "grade",
        "learner_type",
        "survey_type",
        "schema_version",
        "source_file",
        "source_group",
        "source_dataset",
    }:
        return True
    return False


def is_boolean_like_name(name: str) -> bool:
    lowered = name.lower()
    return any(token in lowered for token in BOOLEAN_NAME_TOKENS)


def is_demographic_gender_column(name: str) -> bool:
    lowered = name.lower()
    return lowered in {"gender", "sex"}


def clean_string_columns(df: DataFrame) -> DataFrame:
    expressions = []
    for field in df.schema.fields:
        if not isinstance(field.dataType, T.StringType):
            expressions.append(F.col(field.name))
            continue

        name = field.name
        cleaned = F.trim(F.regexp_replace(F.regexp_replace(F.col(name), r"[\r\n]+", " "), r"\s+", " "))
        cleaned = F.when(F.lower(cleaned).isin(*MISSING_STRINGS), F.lit(None)).otherwise(cleaned)

        if is_text_column(name):
            cleaned = F.regexp_replace(cleaned, r"(?i)[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", "[EMAIL]")
            cleaned = F.regexp_replace(cleaned, r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)(?!\d)", "[PHONE]")

        lower_value = F.lower(cleaned)
        if is_demographic_gender_column(name):
            cleaned = (
                F.when(lower_value.isin("male", "m", "man", "boy"), F.lit("male"))
                .when(lower_value.isin("female", "f", "woman", "girl"), F.lit("female"))
                .when(cleaned.isNull(), F.lit("unknown"))
                .otherwise(F.lit("other"))
            )
        elif is_boolean_like_name(name):
            cleaned = (
                F.when(lower_value.isin(*BOOLEAN_YES_STRINGS), F.lit("yes"))
                .when(lower_value.isin(*BOOLEAN_NO_STRINGS), F.lit("no"))
                .when(lower_value.isin(*BOOLEAN_TRUE_STRINGS), F.lit("true"))
                .when(lower_value.isin(*BOOLEAN_FALSE_STRINGS), F.lit("false"))
                .when(cleaned.isNull(), F.lit("unknown"))
                .otherwise(cleaned)
            )
        else:
            cleaned = (
                F.when(lower_value.isin(*TEXT_YES_STRINGS), F.lit("yes"))
                .when(lower_value.isin(*TEXT_NO_STRINGS), F.lit("no"))
                .when(lower_value.isin(*TEXT_TRUE_STRINGS), F.lit("true"))
                .when(lower_value.isin(*TEXT_FALSE_STRINGS), F.lit("false"))
                .otherwise(cleaned)
            )

        expressions.append(cleaned.alias(name))
    return df.select(*expressions)


def clean_missing_numeric_columns(df: DataFrame) -> DataFrame:
    expressions = []
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            expressions.append(F.col(field.name))
            continue
        expressions.append(
            F.when(F.lower(F.col(field.name).cast("string")).isin(*MISSING_STRINGS), F.lit(None))
            .otherwise(F.col(field.name))
            .alias(field.name)
        )
    return df.select(*expressions)


def numeric_candidate_columns(df: DataFrame) -> List[str]:
    candidates = []
    excluded = DEDUP_METADATA_COLUMNS | SENSITIVE_IDENTIFIERS
    for field in df.schema.fields:
        if not isinstance(field.dataType, T.StringType):
            continue
        name = field.name
        if name in excluded or is_text_column(name) or is_sensitive_identifier(name):
            continue
        if NUMERIC_NAME_PATTERN.match(name):
            candidates.append(name)
    return candidates


def cast_numeric_candidates(df: DataFrame) -> Tuple[DataFrame, List[str]]:
    candidates = numeric_candidate_columns(df)
    if not candidates:
        return df, []

    numeric_regex = r"^[+-]?(?:\d+\.?\d*|\.\d+)$"
    converted: List[str] = []
    result = df
    for name in candidates:
        normalized = F.regexp_replace(F.col(name).cast("string"), ",", "")
        result = result.withColumn(
            name,
            F.when(normalized.rlike(numeric_regex), normalized.cast(T.DoubleType())).otherwise(F.lit(None).cast(T.DoubleType())),
        )
        converted.append(name)
    return result, converted


def parse_datetime_value(name: str):
    value = F.col(name).cast("string")
    parsed = F.coalesce(
        F.to_timestamp(value),
        F.to_timestamp(value, "yyyy-MM-dd"),
        F.to_timestamp(value, "MM/dd/yyyy"),
        F.to_timestamp(value, "M/d/yyyy"),
        F.to_timestamp(value, "dd/MM/yyyy"),
        F.to_timestamp(value, "d/M/yyyy"),
        F.to_timestamp(value, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(value, "MM/dd/yyyy HH:mm:ss"),
    )
    return parsed


def add_date_metadata(df: DataFrame, *, enable_date_parse_verify: bool = False) -> Tuple[DataFrame, List[str]]:
    result = df
    candidates = [name for name in result.columns if DATE_NAME_PATTERN.search(name)]
    parsed_dates = []

    for name in candidates:
        parsed = parse_datetime_value(name)
        if "timestamp" in name or name.endswith("_at") or name.endswith("_time"):
            result = result.withColumn(name, parsed)
        else:
            result = result.withColumn(name, F.to_date(parsed))
        parsed_dates.append(F.to_date(F.col(name)))

    if parsed_dates:
        result = result.withColumn("date", F.coalesce(*parsed_dates))
        if enable_date_parse_verify:
            parsed_date_count = result.select(F.count(F.col("date")).alias("parsed_date_count")).first()["parsed_date_count"]
        else:
            parsed_date_count = None
        if parsed_date_count == 0:
            print("WARNING: Date-like columns exist but no valid survey date was parsed; using ingestion_date partition.")
            result = result.drop("date").withColumn("ingestion_date", F.current_date())
            return result, ["ingestion_date"]
        result = result.withColumn("year", F.year("date")).withColumn("month", F.month("date")).withColumn("day", F.dayofmonth("date"))
        return result, ["year", "month"]

    result = result.withColumn("ingestion_date", F.current_date())
    return result, ["ingestion_date"]


def protect_identifiers(df: DataFrame) -> DataFrame:
    identifier_columns = [name for name in df.columns if is_sensitive_identifier(name)]
    if not identifier_columns:
        return df

    any_identifier = non_null_any_expression(identifier_columns)
    result = df.withColumn(
        "anonymous_id",
        F.when(
            any_identifier,
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(name).cast("string"), F.lit("")) for name in identifier_columns]), 256),
        ),
    )
    return result.drop(*identifier_columns)


def payload_columns(df: DataFrame) -> List[str]:
    return [
        column
        for column in df.columns
        if column not in DEDUP_METADATA_COLUMNS and not column.endswith("_hash") and column != "anonymous_id"
    ]


def non_null_any_expression(columns: Iterable[str]):
    expressions = [F.col(column).isNotNull() for column in columns]
    if not expressions:
        return F.lit(False)
    return reduce(lambda left, right: left | right, expressions)


def non_empty_payload_expression(columns: Iterable[str]):
    expressions = [
        F.length(F.trim(F.coalesce(F.col(column).cast("string"), F.lit("")))) > 0
        for column in columns
    ]
    if not expressions:
        return F.lit(False)
    return reduce(lambda left, right: left | right, expressions)


def build_invalid_empty_payload(raw: DataFrame, columns: List[str]) -> DataFrame:
    payload_columns = columns[:200]
    raw_payload = (
        F.to_json(F.struct(*[F.col(column).cast("string").alias(column) for column in payload_columns]))
        if payload_columns
        else F.lit("{}")
    )
    return raw.select(
        F.col("source_file"),
        F.lit("empty_survey_payload").alias("error_reason"),
        raw_payload.alias("raw_payload"),
        F.current_timestamp().alias("processed_at"),
    )


def deduplicate_by_payload_hash(df: DataFrame, columns: List[str]) -> DataFrame:
    if not columns:
        return df.dropDuplicates()
    if "anonymous_id" in df.columns and "source_file" in df.columns:
        identified = df.where(F.col("anonymous_id").isNotNull()).dropDuplicates(["source_file", "anonymous_id"])
        anonymous = df.where(F.col("anonymous_id").isNull())
        return identified.unionByName(anonymous, allowMissingColumns=True)

    # Standardized research CSV rows are anonymous respondents. Identical answer payloads can be
    # valid different students, so payload-level dedup would collapse the dashboard population.
    return df


def data_quality_summary(
    cleaned: DataFrame,
    raw_rows: int,
    valid_rows_before_dedup: int,
    clean_rows: int,
    invalid_rows: int,
    duplicate_rows: int,
    converted_numeric: Iterable[str],
) -> None:
    categorical_columns = [
        field.name
        for field in cleaned.schema.fields
        if isinstance(field.dataType, (T.StringType, T.BooleanType)) and field.name not in DEDUP_METADATA_COLUMNS
    ]
    numeric_columns = [
        field.name
        for field in cleaned.schema.fields
        if isinstance(field.dataType, T.NumericType) and field.name not in DEDUP_METADATA_COLUMNS
    ]

    print("\nDATA QUALITY SUMMARY - SURVEY BRONZE TO SILVER")
    print(f"total raw rows: {raw_rows}")
    print(f"valid rows before dedup: {valid_rows_before_dedup}")
    print(f"total rows after cleaning: {clean_rows}")
    print(f"invalid rows: {invalid_rows}")
    print(f"duplicate rows removed: {duplicate_rows}")
    print(f"number of columns: {len(cleaned.columns)}")
    print(f"numeric columns converted from string: {list(converted_numeric)}")
    print(f"categorical columns: {categorical_columns}")
    print(f"numeric columns: {numeric_columns}")
    print("schema:")
    cleaned.printSchema()

    print("null count by column:")
    for start in range(0, len(cleaned.columns), 200):
        chunk = cleaned.columns[start : start + 200]
        print(f"null count columns {start + 1}-{start + len(chunk)}:")
        cleaned.agg(*[F.sum(F.col(column).isNull().cast("int")).alias(column) for column in chunk]).show(truncate=False)

    print("sample 20 rows:")
    cleaned.show(20, truncate=80)

    if "source_file" in cleaned.columns:
        print("count by source_file:")
        cleaned.groupBy("source_file").count().orderBy("source_file").show(truncate=False)
    if "source_group" in cleaned.columns:
        print("count by source_group:")
        cleaned.groupBy("source_group").count().orderBy("source_group").show(truncate=False)


def main() -> None:
    args = parse_args()
    if not args.fast_mode:
        args.enable_quality_report = True
        args.enable_output_verify = True
        args.write_invalid_records = True

    start_time = time.time()
    run_id = args.run_id or default_run_id()
    versioned_output = bool(args.versioned_output or args.run_id)
    output_path = versioned_path(args.output_path, run_id, versioned_output)
    write_mode = effective_write_mode(args.write_mode, versioned_output)
    spark = (
        SparkSession.builder.appName("survey-bronze-to-silver-cleaning")
        .config("spark.sql.shuffle.partitions", str(max(1, args.shuffle_partitions)))
        .config("spark.default.parallelism", str(max(1, args.spark_parallelism)))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # The standardized survey union still has wide cleaning expressions. Disabling whole-stage
        # codegen avoids Janino 64 KB generated-method fallbacks in Dataproc driver logs.
        .config("spark.sql.codegen.wholeStage", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = read_bronze_survey_inputs(
            spark,
            args.input_path,
            args.app_snapshot_path,
            args.include_app_snapshot,
        )

        survey_columns = [column for column in raw.columns if column not in {"source_file", "source_group", "source_dataset"}]
        bounded_non_empty_subset = survey_columns[:200]
        if bounded_non_empty_subset:
            has_payload = non_empty_payload_expression(bounded_non_empty_subset)
            invalid_raw = build_invalid_empty_payload(raw.where(~has_payload), bounded_non_empty_subset)
            non_empty_raw = raw.where(has_payload)
        else:
            invalid_raw = build_invalid_empty_payload(raw, bounded_non_empty_subset)
            non_empty_raw = raw.limit(0)
        invalid_rows = "not_counted_for_speed"
        if args.write_invalid_records:
            print(f"Writing invalid survey records to {args.invalid_output_path} with mode={write_mode} without pre-count.")
            invalid_raw.write.mode(write_mode).parquet(args.invalid_output_path)
        else:
            print("Skipping invalid survey output in fast production mode. Use --write-invalid-records to write it.")

        cleaned = clean_string_columns(non_empty_raw)
        cleaned = clean_missing_numeric_columns(cleaned)
        cleaned, converted_numeric = cast_numeric_candidates(cleaned)
        cleaned = protect_identifiers(cleaned)

        with_dates, partition_columns = add_date_metadata(cleaned, enable_date_parse_verify=args.enable_quality_report)
        dedup_payload = payload_columns(with_dates)
        cleaned = deduplicate_by_payload_hash(with_dates, dedup_payload)

        cleaned = (
            cleaned.withColumn("processed_at", F.current_timestamp())
            .withColumn("source_layer", F.lit("bronze"))
            .withColumn("target_layer", F.lit("silver"))
            .withColumn("is_valid", F.lit(True))
        )

        print(f"Writing Silver record-level Parquet only to {output_path} with mode={write_mode}")
        print(f"Partition columns: {partition_columns}")
        output_partitions = max(1, args.output_partitions)
        print(f"Silver output partitions before partitionBy: {output_partitions}")
        print("Silver repartition strategy: hash partition by output partition columns before partitionBy.")
        cleaned.repartition(output_partitions, *[F.col(column) for column in partition_columns]).write.mode(write_mode).partitionBy(*partition_columns).parquet(output_path)
        print("Silver output completed. This job did not write any Gold table.")

        raw_rows = "not_counted_for_speed"
        before_deduplicate = "not_counted_for_speed"
        duplicate_rows = "not_counted_for_speed"
        output_rows = "not_counted_for_speed"
        if args.enable_quality_report:
            quality_frame = cleaned.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                raw_rows = raw.count()
                before_deduplicate = with_dates.count()
                clean_rows = quality_frame.count()
                invalid_rows = invalid_raw.count()
                duplicate_rows = before_deduplicate - clean_rows
                data_quality_summary(
                    quality_frame,
                    raw_rows,
                    before_deduplicate,
                    clean_rows,
                    invalid_rows,
                    duplicate_rows,
                    converted_numeric,
                )
                output_rows = clean_rows
            finally:
                quality_frame.unpersist()
        elif args.enable_output_verify:
            output_rows = spark.read.parquet(output_path).count()

        print_json_log(
            {
                "job_name": "survey_bronze_to_silver",
                "project_id": PROJECT_ID,
                "input_path": args.input_path,
                "app_snapshot_path": args.app_snapshot_path,
                "app_snapshot_included": args.include_app_snapshot,
                "output_path": output_path,
                "invalid_output_path": args.invalid_output_path,
                "requested_write_mode": args.write_mode,
                "effective_write_mode": write_mode,
                "versioned_output": versioned_output,
                "run_id": run_id,
                "fast_mode": args.fast_mode,
                "quality_report_enabled": args.enable_quality_report,
                "output_verify_enabled": args.enable_output_verify,
                "write_invalid_records": args.write_invalid_records,
                "input_rows": raw_rows,
                "valid_rows": before_deduplicate,
                "invalid_rows": invalid_rows,
                "duplicate_removed": duplicate_rows,
                "output_rows": output_rows,
                "output_success": True,
                "output_partitions": output_partitions,
                "spark_parallelism": max(1, args.spark_parallelism),
                "spark_shuffle_partitions": max(1, args.shuffle_partitions),
                "partition_strategy": "hash_repartition_by_output_partition_columns",
                "partition_columns": partition_columns,
                "duration_seconds": round(time.time() - start_time, 2),
                "status": "success",
            }
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
