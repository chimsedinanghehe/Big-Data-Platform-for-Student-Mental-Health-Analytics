"""Silver -> Gold survey dashboard job for Dataproc Serverless Spark.

Scope:
- Read only record-level Silver survey data.
- Build dashboard-ready feature and summary tables.
- Write only to gold/dashboard_tables survey paths.
- Never read Bronze and never process chat, RAG, vectors, or sentiment datasets.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from functools import reduce
from typing import Dict, Iterable, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException
from pyspark import StorageLevel


PROJECT_ID = "student-mental-health-496205"
BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
SILVER_SURVEY_PATH = "gs://student-mental-health-lake-nhom1-2026/silver/survey_cleaned/"
GOLD_SURVEY_OVERVIEW_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_overview_summary/"
GOLD_SURVEY_RESPONSE_BY_DATE_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_response_by_date/"
GOLD_SURVEY_DEMOGRAPHIC_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_demographic_summary/"
GOLD_SURVEY_QUESTION_DISTRIBUTION_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_question_distribution/"
GOLD_SURVEY_NUMERIC_SUMMARY_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_numeric_summary/"
GOLD_SURVEY_ANALYTIC_FEATURES_PATH = "gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_analytic_features/"
TEMP_SURVEY_GOLD_WORK_PATH = "gs://student-mental-health-lake-nhom1-2026/tmp/survey_gold_work/"
WRITE_MODE = "overwrite"

METADATA_COLUMNS = {
    "processed_at",
    "source_layer",
    "target_layer",
    "source_file",
    "source_group",
    "source_dataset",
    "is_valid",
    "date",
    "ingestion_date",
    "year",
    "month",
    "day",
}
TEXT_TOKENS = ("text", "free_text", "comment", "answer", "message", "description", "feedback")
RISK_KEYWORDS = ("stress", "anxiety", "anx", "depression", "depress", "mental_health", "risk")
SCORE_KEYWORDS = ("score", "rating", "scale", "stress", "anxiety", "depression", "mental_health", "q")
QUESTION_PATTERN = re.compile(r"^(q\d+|qn[a-z0-9_]+)$", re.IGNORECASE)

DATA_SOURCE_COLUMN = "Data Source"
POPULATION_COLUMN = "Population"
STUDY_YEAR_COLUMN = "Study Year"
HMS_AGE_COLUMN = "HMS Age"
MENTAL_SCHOOL_POPULATION_LABEL = "Middle/High School"
HMS_POPULATION_LABEL = "College/University"
RESEARCH_FEATURES = [
    "Family Pressure Index",
    "Academic Pressure Index",
    "Peer & Safety Stress Index",
    "Trauma Exposure Index",
    "Substance Coping Risk Index",
    "Lifestyle Recovery Deficit",
]
HMS_NATIVE_FEATURES = [
    "College Financial Strain",
    "College Academic Adjustment",
    "College Belonging Deficit",
    "College Discrimination Exposure",
    "College Campus Safety Stress",
    "College Relationship Harm",
    "College Substance Exposure",
    "College Recovery Deficit",
]
CONSTRUCT_KEYWORDS = {
    "Family Pressure Index": ("family", "parent", "home", "housing", "food", "financial", "q89", "q90", "q91", "q99", "q100", "q101", "q102", "q104"),
    "Academic Pressure Index": ("academic", "school", "grade", "study", "stress", "imposter", "q87", "q103", "q105", "q106"),
    "Peer & Safety Stress Index": ("peer", "safe", "safety", "bully", "belong", "discrim", "q14", "q15", "q18", "q24", "q25"),
    "Trauma Exposure Index": ("trauma", "abuse", "violence", "assault", "stalk", "partner", "q19", "q20", "q21", "q22", "q88"),
    "Substance Coping Risk Index": ("substance", "alcohol", "drug", "smok", "vape", "cig", "mar", "binge", "q33", "q36", "q42", "q43", "q48", "q92"),
    "Lifestyle Recovery Deficit": ("sleep", "exercise", "breakfast", "social", "water", "lifestyle", "q75", "q76", "q80", "q85", "q96"),
    "College Financial Strain": ("housing", "food", "fin", "afford", "pay_worry"),
    "College Academic Adjustment": ("aca", "academic", "stress", "compet", "grade", "imposter", "failed", "adjust", "time_manage", "doubt_school"),
    "College Belonging Deficit": ("belong",),
    "College Discrimination Exposure": ("discrim",),
    "College Campus Safety Stress": ("safe_on", "safe_off", "hostcli"),
    "College Relationship Harm": ("abuse", "stalk", "assault", "ipv", "partner"),
    "College Substance Exposure": ("alc", "binge", "sub", "smok", "drug", "mar"),
    "College Recovery Deficit": ("sleep", "exerc", "food_worry"),
}
ANALYTIC_METADATA_COLUMNS = [
    "source_file",
    "source_group",
    "source_dataset",
    "date",
    "ingestion_date",
    "is_valid",
    "gender",
    "sex",
    "age",
    "grade",
]
SCHOOL_CLUSTER_QNUMS = sorted(
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
SCHOOL_ANALYTIC_COLUMNS = [f"q{qnum}" for qnum in SCHOOL_CLUSTER_QNUMS]
HMS_ANALYTIC_COLUMNS = [
    "age",
    "sex_birth",
    "gender_female",
    "gender_male",
    "gender_nonbin",
    "gender_queer",
    "gender_trans",
    "gender_transm",
    "gender_transf",
    "yr_sch",
    "inst_hmsyear",
    "deprawsc",
    "anx_score",
    "dep_maj",
    "dep_any",
    "anx_any",
    "dep_or_anx",
    "sui_idea",
    "sui_plan",
    "sui_att",
    "fincur",
    "finpast",
    "food_worry",
    "housing",
    "housing1",
    "aca_impa",
    "acad_imp",
    "aca_stress",
    "compet1",
    "imposter_1",
    "imposter_2",
    "imposter_3",
    "imposter_4",
    "imposter_5",
    "doubt_school_1",
    "fam_support_aca",
    "prof_support_aca",
    "adjust_aca_1",
    "adjust_aca_2",
    "failed",
    "time_manage",
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
    "belong",
    "discrim",
    "safe_on",
    "safe_off",
    "hostcli",
    "abuse_life",
    "abuse_recent",
    "stalk_life",
    "stalk_recent",
    "assault_life",
    "assault_recent",
    "assault_sex",
    "assault_sex_y",
    "ipv_1",
    "ipv_2",
    "ipv_3",
    "ipv_4",
    "ipv_5",
    "alc_any",
    "binge_fr",
    "sub_any",
    "sub_cig",
    "smok_freq",
    "drug_mar",
    "mar_freq",
    "sleep_wknight",
    "sleep_wkend",
    "exerc",
    "exerc_range4",
    "exerc_range5",
]
HMS_ANALYTIC_COLUMNS = list(
    dict.fromkeys(
        HMS_ANALYTIC_COLUMNS
        + [
            "housing_worry",
            "stress1",
            "stress2",
            "stress3",
            "stress4",
            "compet_sch",
            "grade_curv",
            "belong1",
            "belong2",
            "belong8",
            "belong9",
            "discrim_race",
            "discrim_culture",
            "discrim_gender",
            "discrim_sexual",
            "discrim_other",
            "safe_on_day",
            "safe_on_night",
            "safe_off_day",
            "safe_off_night",
            "hostcli_distress",
            "stalk_exp",
            "sa_exp",
            "partner_phys",
            "partner_insult",
            "partner_threat",
            "partner_curse",
            "smok_vape",
        ]
    )
)
ANALYTIC_SOURCE_COLUMNS = list(dict.fromkeys(SCHOOL_ANALYTIC_COLUMNS + HMS_ANALYTIC_COLUMNS))
ANALYTIC_OUTPUT_PASSTHROUGH_COLUMNS = list(
    dict.fromkeys(
        # Raw passthrough is only for the school-detail tab question clusters.
        # College detail uses the generated construct columns, so raw HMS helper fields stay out of Gold analytic.
        SCHOOL_ANALYTIC_COLUMNS
    )
)
ANALYTIC_RISK_COLUMNS = [
    "q26",
    "q84",
    "deprawsc",
    "anx_score",
    "dep_maj",
    "dep_any",
    "anx_any",
    "dep_or_anx",
    "sui_idea",
    "sui_plan",
    "sui_att",
]
ANALYTIC_FAST_COLUMNS = [
    "q1",
    "q2",
    "q3",
    "q14",
    "q19",
    "q26",
    "q33",
    "q75",
    "q84",
    "q87",
    "q89",
    "age",
    "sex_birth",
    "gender_female",
    "gender_male",
    "gender_nonbin",
    "gender_queer",
    "gender_trans",
    "gender_transm",
    "gender_transf",
    "yr_sch",
    "inst_hmsyear",
    "deprawsc",
    "anx_score",
    "dep_maj",
    "dep_any",
    "anx_any",
    "dep_or_anx",
    "sui_idea",
    "sui_plan",
    "sui_att",
    "fincur",
    "food_worry",
    "aca_stress",
    "belong",
    "discrim",
    "safe_on",
    "abuse_life",
    "sub_any",
    "sleep_wknight",
    "exerc",
]
FAST_CONSTRUCT_COLUMNS = {
    "Family Pressure Index": ["q89", "food_worry"],
    "Academic Pressure Index": ["q87", "aca_stress"],
    "Peer & Safety Stress Index": ["q14", "belong", "safe_on"],
    "Trauma Exposure Index": ["q19", "abuse_life"],
    "Substance Coping Risk Index": ["q33", "sub_any"],
    "Lifestyle Recovery Deficit": ["q75", "sleep_wknight", "exerc"],
    "College Financial Strain": ["fincur", "food_worry"],
    "College Academic Adjustment": ["aca_stress"],
    "College Belonging Deficit": ["belong"],
    "College Discrimination Exposure": ["discrim"],
    "College Campus Safety Stress": ["safe_on"],
    "College Relationship Harm": ["abuse_life"],
    "College Substance Exposure": ["sub_any"],
    "College Recovery Deficit": ["sleep_wknight", "exerc"],
}
FIXED_CONSTRUCT_COLUMNS = {
    "Family Pressure Index": ["q89", "q90", "q91", "q99", "q100", "q101", "q102", "q104"],
    "Academic Pressure Index": ["q87", "q103", "q105", "q106"],
    "Peer & Safety Stress Index": ["q14", "q15", "q18", "q24", "q25"],
    "Trauma Exposure Index": ["q19", "q20", "q21", "q22", "q88"],
    "Substance Coping Risk Index": ["q33", "q36", "q42", "q43", "q48", "q92"],
    "Lifestyle Recovery Deficit": ["q75", "q76", "q80", "q85", "q96"],
    "College Financial Strain": [
        "fincur",
        "finpast",
        "food_worry",
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
    ],
    "College Academic Adjustment": [
        "aca_impa",
        "acad_imp",
        "aca_stress",
        "compet1",
        "imposter_1",
        "imposter_2",
        "imposter_3",
        "imposter_4",
        "imposter_5",
        "doubt_school_1",
        "fam_support_aca",
        "prof_support_aca",
        "adjust_aca_1",
        "adjust_aca_2",
        "failed",
        "time_manage",
    ],
    "College Belonging Deficit": ["belong"],
    "College Discrimination Exposure": ["discrim"],
    "College Campus Safety Stress": ["safe_on", "safe_off", "hostcli"],
    "College Relationship Harm": [
        "abuse_life",
        "abuse_recent",
        "stalk_life",
        "stalk_recent",
        "assault_life",
        "assault_recent",
        "assault_sex",
        "assault_sex_y",
        "ipv_1",
        "ipv_2",
        "ipv_3",
        "ipv_4",
        "ipv_5",
    ],
    "College Substance Exposure": [
        "alc_any",
        "binge_fr",
        "sub_any",
        "sub_cig",
        "smok_freq",
        "drug_mar",
        "mar_freq",
    ],
    "College Recovery Deficit": ["sleep_wknight", "sleep_wkend", "exerc", "exerc_range4", "exerc_range5"],
}
DASHBOARD_QUESTION_DISTRIBUTION_COLUMNS = list(
    dict.fromkeys(
        [
            "source_group",
            *[f"q{qnum}" for qnum in SCHOOL_CLUSTER_QNUMS],
            "fincur",
            "food_worry",
            "aca_impa",
            "aca_stress",
            "belong",
            "discrim",
            "safe_on",
            "abuse_life",
            "sub_any",
            "sleep_wknight",
            "sleep_wkend",
            "exerc",
        ]
    )
)
DASHBOARD_NUMERIC_SUMMARY_COLUMNS = list(
    dict.fromkeys(
        [
            "age",
            "q1",
            "q2",
            "q3",
            "q26",
            "q27",
            "q28",
            "q29",
            "q84",
            *RESEARCH_FEATURES,
            *HMS_NATIVE_FEATURES,
            "missing_field_count",
            "answer_completeness_rate",
        ]
    )
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create Gold survey dashboard tables from Silver Parquet only.")
    parser.add_argument("--input-path", default=SILVER_SURVEY_PATH)
    parser.add_argument("--process-date", help="Filter Silver survey date/ingestion_date=YYYY-MM-DD for rerun/backfill.")
    parser.add_argument("--overview-output-path", default=GOLD_SURVEY_OVERVIEW_PATH)
    parser.add_argument("--response-by-date-output-path", default=GOLD_SURVEY_RESPONSE_BY_DATE_PATH)
    parser.add_argument("--demographic-output-path", default=GOLD_SURVEY_DEMOGRAPHIC_PATH)
    parser.add_argument("--question-distribution-output-path", default=GOLD_SURVEY_QUESTION_DISTRIBUTION_PATH)
    parser.add_argument("--numeric-summary-output-path", default=GOLD_SURVEY_NUMERIC_SUMMARY_PATH)
    parser.add_argument("--analytic-features-output-path", default=GOLD_SURVEY_ANALYTIC_FEATURES_PATH)
    parser.add_argument("--gold-max-small-table-rows", type=int, default=100000)
    parser.add_argument("--gold-output-partitions", type=int, default=32)
    parser.add_argument("--temp-work-path", default=TEMP_SURVEY_GOLD_WORK_PATH)
    parser.add_argument("--temp-output-partitions", type=int, default=32)
    parser.add_argument(
        "--disable-temp-stage",
        action="store_true",
        help="Skip Stage A compact temp Parquet. Use only for debugging; production path keeps temp stage enabled.",
    )
    parser.add_argument("--run-id", help="Versioned Gold run id. Default is UTC timestamp.")
    parser.add_argument(
        "--disable-versioned-output",
        action="store_true",
        help="Write directly to table paths. Slower on GCS when write-mode=overwrite; only use for manual cleanup workflows.",
    )
    parser.add_argument("--write-mode", default=WRITE_MODE, choices=["overwrite", "append", "errorifexists", "ignore"])
    return parser.parse_args()


def print_json_log(payload: dict) -> None:
    print("JOB_JSON_LOG " + json.dumps(payload, default=str, sort_keys=True))


def default_run_id() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def versioned_path(path: str, run_id: str, enabled: bool) -> str:
    if not enabled:
        return path
    return f"{path.rstrip('/')}/run_id={run_id}/"


def compact_temp_path(base_path: str, run_id: str) -> str:
    return f"{base_path.rstrip('/')}/survey_silver_compact/run_id={run_id}/"


def effective_write_mode(write_mode: str, versioned_output: bool) -> str:
    # Versioned paths are new for each run, so errorifexists avoids GCS delete/overwrite commit work.
    if versioned_output and write_mode == "overwrite":
        return "errorifexists"
    return write_mode


def union_frames(frames: List[DataFrame]) -> Optional[DataFrame]:
    if not frames:
        return None
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)


def is_text_column(name: str) -> bool:
    lowered = name.lower()
    return any(token in lowered for token in TEXT_TOKENS)


def is_gold_excluded_column(name: str) -> bool:
    lowered = name.lower()
    return (
        lowered in METADATA_COLUMNS
        or lowered == "anonymous_id"
        or lowered.endswith("_hash")
        or lowered in {"email", "name", "full_name", "phone", "phone_number", "student_id", "user_id"}
    )


def payload_columns(df: DataFrame) -> List[str]:
    return [column for column in df.columns if not is_gold_excluded_column(column) and not is_text_column(column)]


def ensure_source_group(df: DataFrame) -> DataFrame:
    if "source_group" in df.columns:
        return df.withColumn(
            "source_group",
            F.when(F.lower(F.col("source_group")).contains("school"), F.lit("school"))
            .when(F.lower(F.col("source_group")).contains("university"), F.lit("university"))
            .otherwise(F.coalesce(F.col("source_group"), F.lit("unknown"))),
        )

    source_expr = F.lit("unknown")
    if "source_file" in df.columns:
        source_expr = (
            F.when(F.lower(F.col("source_file")).contains("school"), F.lit("school"))
            .when(F.lower(F.col("source_file")).contains("university"), F.lit("university"))
            .otherwise(F.lit("unknown"))
        )
    elif "source_dataset" in df.columns:
        source_expr = (
            F.when(F.lower(F.col("source_dataset")).contains("school"), F.lit("school"))
            .when(F.lower(F.col("source_dataset")).contains("university"), F.lit("university"))
            .otherwise(F.lit("unknown"))
        )
    return df.withColumn("source_group", source_expr)


def valid_condition(df: DataFrame):
    return F.col("is_valid") == F.lit(True) if "is_valid" in df.columns else F.lit(True)


def build_overview(df: DataFrame) -> DataFrame:
    payload = payload_columns(df)
    total_cells = len(payload)
    non_null_exprs = [F.count(F.col(column)).alias(f"__nn_{idx}") for idx, column in enumerate(payload)]
    row = (
        df.agg(
            F.count(F.lit(1)).alias("total_responses"),
            F.sum(F.when(valid_condition(df), 1).otherwise(0)).alias("valid_responses"),
            F.sum(F.when(F.col("source_group") == "school", 1).otherwise(0)).alias("school_responses"),
            F.sum(F.when(F.col("source_group") == "university", 1).otherwise(0)).alias("university_responses"),
            *non_null_exprs,
        )
        .first()
        .asDict()
    )

    total_responses = int(row.get("total_responses") or 0)
    valid_responses = int(row.get("valid_responses") or 0)
    school_responses = int(row.get("school_responses") or 0)
    university_responses = int(row.get("university_responses") or 0)
    non_null_cells = sum(int(row.get(f"__nn_{idx}") or 0) for idx, _ in enumerate(payload))
    denominator = total_responses * total_cells
    missing_rate = round((denominator - non_null_cells) / denominator * 100, 2) if denominator else 0.0

    return df.sparkSession.createDataFrame(
        [
            (
                total_responses,
                total_cells,
                valid_responses,
                total_responses - valid_responses,
                missing_rate,
                school_responses,
                university_responses,
            )
        ],
        [
            "total_responses",
            "total_columns",
            "valid_responses",
            "invalid_responses",
            "missing_value_rate",
            "school_responses",
            "university_responses",
        ],
    ).withColumn("processed_at", F.current_timestamp())


def build_response_by_date(df: DataFrame) -> Optional[DataFrame]:
    date_exprs = []
    if "date" in df.columns:
        date_exprs.append(F.col("date").cast("date"))
    if "ingestion_date" in df.columns:
        date_exprs.append(F.col("ingestion_date").cast("date"))
    if not date_exprs:
        print("WARNING: date/ingestion_date column not found; use current_date for survey_response_by_date.")
        date_exprs.append(F.current_date())
    date_expr = F.coalesce(*date_exprs, F.current_date()).alias("date")

    return (
        df.groupBy(date_expr)
        .agg(
            F.count(F.lit(1)).alias("total_responses"),
            F.sum(F.when(valid_condition(df), 1).otherwise(0)).alias("valid_responses"),
            F.sum(F.when(F.col("source_group") == "school", 1).otherwise(0)).alias("school_responses"),
            F.sum(F.when(F.col("source_group") == "university", 1).otherwise(0)).alias("university_responses"),
        )
        .orderBy("date")
    )


def with_age_group(df: DataFrame) -> DataFrame:
    if "age" not in df.columns:
        return df.withColumn("age_group", F.lit("unknown"))

    age = F.col("age").cast("double")
    return df.withColumn(
        "age_group",
        F.when(age.isNull(), F.lit("unknown"))
        .when(age < 13, F.lit("under_13"))
        .when(age.between(13, 15), F.lit("13_15"))
        .when(age.between(16, 18), F.lit("16_18"))
        .when(age.between(19, 24), F.lit("19_24"))
        .when(age.between(25, 34), F.lit("25_34"))
        .otherwise(F.lit("35_plus")),
    )


def value_distribution(df: DataFrame, columns: Iterable[str], name_col: str, value_col: str) -> Optional[DataFrame]:
    frames: List[DataFrame] = []
    total = max(df.count(), 1)
    for column in columns:
        if column not in df.columns:
            continue
        frames.append(
            df.where(F.col(column).isNotNull())
            .groupBy(F.col(column).cast("string").alias(value_col))
            .agg(F.count(F.lit(1)).alias("count"))
            .withColumn(name_col, F.lit(column))
            .withColumn("percentage", F.round(F.col("count") / F.lit(total) * 100, 2))
            .select(name_col, value_col, "count", "percentage")
        )
    return union_frames(frames)


def build_demographic_summary(df: DataFrame) -> Optional[DataFrame]:
    working = with_age_group(df)
    dimensions = [column for column in ["gender", "sex", "age", "age_group", "grade", "source_group"] if column in working.columns]
    if not dimensions:
        print("WARNING: Skip survey_demographic_summary because gender/sex/age/grade/source_group columns not found.")
        return None
    return value_distribution(working, dimensions, "dimension_name", "dimension_value")


def string_categorical_columns(df: DataFrame) -> List[str]:
    candidates = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, (T.StringType, T.BooleanType))
        and not is_gold_excluded_column(field.name)
        and not is_text_column(field.name)
    ]
    if not candidates:
        return []

    stats = df.agg(*[F.approx_count_distinct(F.col(column)).alias(column) for column in candidates]).first().asDict()
    return [column for column in candidates if int(stats.get(column) or 0) <= 100]


def numeric_question_columns(df: DataFrame) -> List[str]:
    return [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, T.NumericType)
        and not is_gold_excluded_column(field.name)
        and QUESTION_PATTERN.match(field.name)
    ]


def is_numeric_named_column(name: str) -> bool:
    lowered = name.lower()
    if QUESTION_PATTERN.match(name):
        return True
    if lowered in {"age", "grade", "yr_sch", "inst_hmsyear", "sex_birth"}:
        return True
    if any(keyword in lowered for keyword in ("score", "scale", "rating", "count", "total", "rawsc")):
        return True
    return any(
        keyword == lowered or keyword in lowered
        for keywords in CONSTRUCT_KEYWORDS.values()
        for keyword in keywords
    )


def build_question_distribution(df: DataFrame) -> Optional[DataFrame]:
    columns = [column for column in DASHBOARD_QUESTION_DISTRIBUTION_COLUMNS if column in df.columns]
    if not columns:
        columns = sorted(set(string_categorical_columns(df) + numeric_question_columns(df)))[:60]
    if not columns:
        print("WARNING: Skip survey_question_distribution because no categorical/question columns were found.")
        return None

    frames: List[DataFrame] = []
    source_window = Window.partitionBy("source_group")
    for column in columns:
        grouped = (
            df.where(F.col(column).isNotNull())
            .groupBy("source_group", F.col(column).cast("string").alias("answer_value"))
            .agg(F.count(F.lit(1)).alias("count"))
            .withColumn("column_name", F.lit(column))
            .withColumn("__column_total", F.sum("count").over(source_window))
            .withColumn("percentage", F.round(F.col("count") / F.col("__column_total") * 100, 2))
            .select("column_name", "answer_value", "source_group", "count", "percentage")
        )
        frames.append(grouped)
    return union_frames(frames)


def numeric_columns(df: DataFrame) -> List[str]:
    return [
        field.name
        for field in df.schema.fields
        if not is_gold_excluded_column(field.name)
        and (
            isinstance(field.dataType, T.NumericType)
            or (isinstance(field.dataType, T.StringType) and not is_text_column(field.name) and is_numeric_named_column(field.name))
        )
    ]


def build_numeric_summary(df: DataFrame) -> Optional[DataFrame]:
    columns = [column for column in DASHBOARD_NUMERIC_SUMMARY_COLUMNS if column in df.columns]
    if not columns:
        columns = numeric_columns(df)[:80]
    if not columns:
        print("WARNING: Skip survey_numeric_summary because no numeric columns were found.")
        return None

    frames = []
    for column in columns:
        frames.append(
            df.groupBy("source_group")
            .agg(
                F.count(F.col(column).cast("double")).alias("count"),
                F.avg(F.col(column).cast("double")).alias("avg"),
                F.min(F.col(column).cast("double")).alias("min"),
                F.max(F.col(column).cast("double")).alias("max"),
                F.stddev(F.col(column).cast("double")).alias("stddev"),
            )
            .withColumn("column_name", F.lit(column))
            .select("column_name", "source_group", "count", "avg", "min", "max", "stddev")
        )
    return union_frames(frames)


def find_columns(df: DataFrame, keywords: Iterable[str]) -> List[str]:
    lowered = [keyword.lower() for keyword in keywords]
    return [
        column
        for column in df.columns
        if not is_gold_excluded_column(column)
        and not is_text_column(column)
        and any(keyword in column.lower() for keyword in lowered)
    ]


def boolean_or(expressions: Iterable):
    values = list(expressions)
    if not values:
        return F.lit(False)
    return reduce(lambda left, right: left | right, values)


def build_score_thresholds(df: DataFrame, columns: List[str]) -> Dict[str, float]:
    if not columns:
        return {}
    row = df.agg(*[F.max(F.col(column).cast("double")).alias(column) for column in columns]).first().asDict()
    thresholds: Dict[str, float] = {}
    for column in columns:
        max_value = row.get(column)
        if max_value is None:
            continue
        max_value = float(max_value)
        if max_value <= 5:
            thresholds[column] = 4.0
        elif max_value <= 10:
            thresholds[column] = 7.0
        else:
            thresholds[column] = round(max_value * 0.75, 4)
    return thresholds


def build_risk_flag(df: DataFrame, risk_columns: List[str], score_thresholds: Dict[str, float]):
    expressions = []
    for column in risk_columns:
        field = df.schema[column]
        if isinstance(field.dataType, T.NumericType):
            threshold = score_thresholds.get(column, 4.0)
            expressions.append(F.col(column).cast("double") >= F.lit(threshold))
        else:
            value = F.lower(F.col(column).cast("string"))
            expressions.append(value.rlike(r"(yes|true|high|severe|stress|anxiety|depression|risk)"))
    return boolean_or(expressions)


def build_high_score_flag(df: DataFrame, score_columns: List[str], score_thresholds: Dict[str, float]):
    expressions = []
    for column in score_columns:
        threshold = score_thresholds.get(column)
        if threshold is not None:
            expressions.append(F.col(column).cast("double") >= F.lit(threshold))
    return boolean_or(expressions)


def existing_columns(df: DataFrame, names: Sequence[str]) -> List[str]:
    return [name for name in names if name in df.columns]


def first_existing_numeric(df: DataFrame, names: Sequence[str]):
    columns = existing_columns(df, names)
    if not columns:
        return F.lit(None).cast("double")
    return F.coalesce(*[F.col(column).cast("double") for column in columns])


def source_dataset_expr(df: DataFrame):
    if "source_dataset" in df.columns:
        return F.col("source_dataset").cast("string")
    if "source_file" in df.columns:
        return F.regexp_replace(F.col("source_file").cast("string"), r"\.csv$", "")
    return F.col("source_group").cast("string")


def data_source_expr(df: DataFrame):
    base = source_dataset_expr(df)
    return (
        F.when(F.lower(base).contains("university 1"), F.lit("HMS 2022-2023"))
        .when(F.lower(base).contains("university 2"), F.lit("HMS 2023-2024"))
        .when(F.lower(base).contains("university 3"), F.lit("HMS 2024-2025"))
        .when(F.lower(base).contains("school"), F.lit("Mental School"))
        .otherwise(base)
    )


def population_expr():
    return (
        F.when(F.col("source_group") == "school", F.lit(MENTAL_SCHOOL_POPULATION_LABEL))
        .when(F.col("source_group") == "university", F.lit(HMS_POPULATION_LABEL))
        .otherwise(F.lit("Unknown"))
    )


def age_to_q1(age_expr):
    return (
        F.when(age_expr < 13, 1.0)
        .when(age_expr == 13, 2.0)
        .when(age_expr == 14, 3.0)
        .when(age_expr == 15, 4.0)
        .when(age_expr == 16, 5.0)
        .when(age_expr == 17, 6.0)
        .when(age_expr == 18, 7.0)
        .when(age_expr.between(19, 20), 8.0)
        .when(age_expr.between(21, 24), 9.0)
        .when(age_expr.between(25, 34), 10.0)
        .when(age_expr >= 35, 11.0)
    )


def gender_to_q2(df: DataFrame):
    q2 = first_existing_numeric(df, ["q2"])
    if "gender" in df.columns:
        gender = F.lower(F.col("gender").cast("string"))
        q2 = F.coalesce(
            q2,
            F.when(gender == "female", 1.0)
            .when(gender == "male", 2.0)
            .when(gender.isin("other", "unknown"), 3.0),
        )
    if "sex" in df.columns:
        sex = F.lower(F.col("sex").cast("string"))
        q2 = F.coalesce(
            q2,
            F.when(sex == "female", 1.0)
            .when(sex == "male", 2.0)
            .when(sex.isin("other", "unknown"), 3.0),
        )
    return F.coalesce(
        q2,
        F.when(first_existing_numeric(df, ["gender_female"]) == 1, 1.0)
        .when(first_existing_numeric(df, ["gender_male"]) == 1, 2.0)
        .when(
            boolean_or(
                [
                    first_existing_numeric(df, ["gender_nonbin"]) == 1,
                    first_existing_numeric(df, ["gender_queer"]) == 1,
                    first_existing_numeric(df, ["gender_trans"]) == 1,
                    first_existing_numeric(df, ["gender_transm"]) == 1,
                    first_existing_numeric(df, ["gender_transf"]) == 1,
                ]
            ),
            3.0,
        ),
        first_existing_numeric(df, ["sex_birth"]),
    )


def grade_to_q3(df: DataFrame):
    grade = first_existing_numeric(df, ["q3", "grade", "yr_sch"])
    return (
        F.when(grade.isNull(), F.lit(None).cast("double"))
        .when((F.col("source_group") == "school") & grade.between(9, 12), grade - F.lit(8.0))
        .when((F.col("source_group") == "school") & grade.between(1, 5), grade)
        .when((F.col("source_group") == "university") & (grade == 1), 6.0)
        .when((F.col("source_group") == "university") & (grade == 2), 7.0)
        .when((F.col("source_group") == "university") & (grade == 3), 8.0)
        .when((F.col("source_group") == "university") & (grade.isin(4, 5)), 9.0)
        .when((F.col("source_group") == "university") & (grade == 6), 10.0)
        .when((F.col("source_group") == "university") & (grade == 7), 11.0)
        .otherwise(grade)
    )


def normalize_numeric_for_construct(df: DataFrame, column: str, max_values: Dict[str, float]):
    value = F.col(column).cast("double")
    max_value = float(max_values.get(column) or 0.0)
    if max_value <= 1.0:
        return F.when(value.isNotNull(), value)
    if max_value <= 5.0:
        return F.when(value.isNotNull(), (value - F.lit(1.0)) / F.lit(max(max_value - 1.0, 1.0)))
    return F.when(value.isNotNull(), value / F.lit(max_value))


def mean_or_null(expressions: Sequence):
    values = [value for value in expressions if value is not None]
    if not values:
        return F.lit(None).cast("double")
    present = [F.when(value.isNotNull(), 1).otherwise(0) for value in values]
    total_present = reduce(lambda left, right: left + right, present)
    total_value = reduce(lambda left, right: left + right, [F.coalesce(value, F.lit(0.0)) for value in values])
    return F.when(total_present > 0, F.round(total_value / total_present * 100, 4))


def construct_columns(df: DataFrame, keywords: Sequence[str]) -> List[str]:
    lowered = [keyword.lower() for keyword in keywords]
    columns = []
    for field in df.schema.fields:
        if not isinstance(field.dataType, (T.NumericType, T.StringType)):
            continue
        name = field.name
        name_lower = name.lower()
        if is_gold_excluded_column(name) or is_text_column(name) or name in {"Target", "missing_field_count", "answer_completeness_rate"}:
            continue
        if any(keyword == name_lower or keyword in name_lower for keyword in lowered):
            columns.append(name)
    return columns


def construct_expressions(df: DataFrame, max_values: Dict[str, float]) -> Dict[str, object]:
    expressions = {}
    for construct_name, keywords in CONSTRUCT_KEYWORDS.items():
        columns = construct_columns(df, keywords)
        expressions[construct_name] = mean_or_null(
            [normalize_numeric_for_construct(df, column, max_values) for column in columns]
        )
    return expressions


def numeric_max_values(df: DataFrame, columns: Sequence[str]) -> Dict[str, float]:
    if not columns:
        return {}
    row = df.agg(*[F.max(F.col(column).cast("double")).alias(column) for column in columns]).first().asDict()
    return {column: float(value) for column, value in row.items() if value is not None}


def fast_value_score(column: str):
    value = F.col(column).cast("double")
    return (
        F.when(value.isNull(), F.lit(None).cast("double"))
        .when(value.between(0, 1), value * F.lit(100.0))
        .when(value.between(1, 5), (value - F.lit(1.0)) / F.lit(4.0) * F.lit(100.0))
        .otherwise(value)
    )


def fast_construct_expression(df: DataFrame, columns: Sequence[str]):
    values = [fast_value_score(column) for column in existing_columns(df, columns)]
    return mean_or_null(values)


def optional_first_numeric(df: DataFrame, names: Sequence[str]):
    columns = existing_columns(df, names)
    if not columns:
        return None
    return F.coalesce(*[F.col(column).cast("double") for column in columns])


def clamp_unit_interval(expression):
    return F.when(expression.isNotNull(), F.least(F.greatest(expression, F.lit(0.0)), F.lit(1.0)))


def ordered_risk_from_first(df: DataFrame, names: Sequence[str], min_value: float, max_value: float, reverse: bool = False):
    value = optional_first_numeric(df, names)
    if value is None or max_value <= min_value:
        return None
    risk = F.when(value.between(min_value, max_value), (value - F.lit(min_value)) / F.lit(max_value - min_value))
    if reverse:
        risk = F.lit(1.0) - risk
    return clamp_unit_interval(risk)


def ordered_mean_risk(df: DataFrame, names: Sequence[str], min_value: float, max_value: float, reverse: bool = False):
    return mean_or_null(
        [ordered_risk_from_first(df, [name], min_value, max_value, reverse=reverse) for name in names if name in df.columns]
    )


def binary_risk_from_first(df: DataFrame, names: Sequence[str]):
    columns = existing_columns(df, names)
    if not columns:
        return None
    values = []
    for column in columns:
        raw = F.col(column).cast("string")
        numeric = F.col(column).cast("double")
        text = F.lower(F.trim(raw))
        values.append(
            F.when(numeric == 1, F.lit(1.0))
            .when(numeric.isin(0, 2), F.lit(0.0))
            .when(text.isin("yes", "y", "true", "t"), F.lit(1.0))
            .when(text.isin("no", "n", "false", "f"), F.lit(0.0))
        )
    return F.coalesce(*values)


def binary_any_risk(df: DataFrame, names: Sequence[str]):
    values = [binary_risk_from_first(df, [name]) for name in names if name in df.columns]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return F.greatest(*values)


def mapped_risk_from_first(df: DataFrame, names: Sequence[str], mapping: Dict[float, float]):
    value = optional_first_numeric(df, names)
    if value is None:
        return None
    expression = F.lit(None).cast("double")
    for raw_value, risk_value in mapping.items():
        expression = F.when(value == F.lit(float(raw_value)), F.lit(float(risk_value))).otherwise(expression)
    return expression


def hms_sleep_deficit(df: DataFrame):
    values = []
    for name in ["sleep_wknight", "sleep_wkend"]:
        if name not in df.columns:
            continue
        hours = F.col(name).cast("double")
        valid = F.when(hours.between(1, 12), hours)
        deficit = (
            F.when(valid < 7, (F.lit(7.0) - valid) / F.lit(6.0))
            .when(valid.between(7, 9), F.lit(0.0))
            .when(valid > 9, (valid - F.lit(9.0)) / F.lit(3.0))
        )
        values.append(clamp_unit_interval(deficit))
    if not values:
        return F.lit(None).cast("double")
    present = [F.when(value.isNotNull(), 1).otherwise(0) for value in values]
    total_present = reduce(lambda left, right: left + right, present)
    total_value = reduce(lambda left, right: left + right, [F.coalesce(value, F.lit(0.0)) for value in values])
    return F.when(total_present > 0, total_value / total_present)


def semantic_construct_expression(df: DataFrame, construct_name: str):
    if construct_name == "Family Pressure Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q89"], 1, 5),
                ordered_risk_from_first(df, ["q90"], 1, 5),
                ordered_risk_from_first(df, ["q91"], 1, 5),
                ordered_risk_from_first(df, ["q99"], 1, 5, reverse=True),
                binary_risk_from_first(df, ["q100"]),
                binary_risk_from_first(df, ["q101"]),
                binary_risk_from_first(df, ["q102"]),
                ordered_risk_from_first(df, ["q104"], 1, 5, reverse=True),
                ordered_risk_from_first(df, ["housing_worry", "housing", "housing1"], 1, 3),
                ordered_risk_from_first(df, ["food_worry"], 1, 3),
                ordered_risk_from_first(df, ["fincur"], 1, 5, reverse=True),
                ordered_risk_from_first(df, ["finpast"], 1, 5, reverse=True),
                ordered_mean_risk(df, ["afford_school", "afford_food", "afford_transp", "afford_hc", "afford_books", "afford_house"], 1, 6),
                ordered_mean_risk(df, ["pay_worry", "pay_worry1", "pay_worry2", "pay_worry3"], 1, 6, reverse=True),
                ordered_mean_risk(df, ["fam_support_aca", "prof_support_aca"], 1, 6),
            ]
        )
    if construct_name == "Academic Pressure Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q87"], 1, 5),
                ordered_risk_from_first(df, ["q103"], 1, 5),
                binary_risk_from_first(df, ["q105"]),
                binary_risk_from_first(df, ["q106"]),
                ordered_risk_from_first(df, ["aca_impa", "acad_imp"], 1, 4),
                ordered_mean_risk(df, ["stress1", "stress2", "stress3", "stress4", "aca_stress"], 1, 5),
                ordered_risk_from_first(df, ["compet_sch", "compet1"], 1, 5, reverse=True),
                ordered_risk_from_first(df, ["grade_curv"], 1, 5),
                ordered_mean_risk(df, ["imposter_1", "imposter_2", "imposter_3", "imposter_4", "imposter_5"], 1, 5),
                binary_risk_from_first(df, ["failed"]),
                ordered_mean_risk(df, ["adjust_aca_1", "adjust_aca_2", "time_manage", "doubt_school_1"], 1, 6),
            ]
        )
    if construct_name == "Peer & Safety Stress Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q14"], 1, 5),
                ordered_risk_from_first(df, ["q15"], 1, 8),
                binary_risk_from_first(df, ["q18"]),
                binary_risk_from_first(df, ["q24"]),
                binary_risk_from_first(df, ["q25"]),
                ordered_mean_risk(df, ["belong1", "belong2", "belong8", "belong9", "belong"], 1, 6),
                binary_any_risk(df, ["discrim_race", "discrim_culture", "discrim_gender", "discrim_sexual", "discrim_other", "discrim"]),
                ordered_mean_risk(df, ["safe_on_day", "safe_on_night", "safe_off_day", "safe_off_night", "safe_on", "safe_off"], 1, 6),
                ordered_risk_from_first(df, ["hostcli_distress", "hostcli"], 1, 5),
            ]
        )
    if construct_name == "Trauma Exposure Index":
        return mean_or_null(
            [
                binary_risk_from_first(df, ["q19"]),
                mapped_risk_from_first(df, ["q20"], {1: 0.0, 2: 0.25, 3: 0.5, 4: 0.75, 5: 1.0}),
                mapped_risk_from_first(df, ["q21"], {1: 0.0, 2: 0.0, 3: 0.25, 4: 0.5, 5: 0.75, 6: 1.0}),
                mapped_risk_from_first(df, ["q22"], {1: 0.0, 2: 0.0, 3: 0.25, 4: 0.5, 5: 0.75, 6: 1.0}),
                binary_risk_from_first(df, ["q88"]),
                ordered_risk_from_first(df, ["abuse_life"], 1, 5),
                ordered_risk_from_first(df, ["abuse_recent"], 1, 5),
                binary_risk_from_first(df, ["stalk_exp", "stalk_life", "stalk_recent"]),
                ordered_risk_from_first(df, ["assault_sex", "assault_sex_y", "assault_life", "assault_recent", "sa_exp"], 1, 4),
                binary_any_risk(df, ["ipv_1", "ipv_2", "ipv_3", "ipv_4", "ipv_5", "partner_phys", "partner_insult", "partner_threat", "partner_curse"]),
            ]
        )
    if construct_name == "Substance Coping Risk Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q33"], 1, 7),
                ordered_risk_from_first(df, ["q36"], 1, 7),
                ordered_risk_from_first(df, ["q42"], 1, 7),
                ordered_risk_from_first(df, ["q43"], 1, 6),
                ordered_risk_from_first(df, ["q48"], 1, 7),
                ordered_risk_from_first(df, ["q92"], 1, 6),
                binary_risk_from_first(df, ["alc_any"]),
                ordered_risk_from_first(df, ["binge_fr"], 1, 6),
                binary_risk_from_first(df, ["sub_any"]),
                binary_risk_from_first(df, ["sub_cig"]),
                ordered_risk_from_first(df, ["smok_freq", "smok_vape"], 1, 5),
                binary_risk_from_first(df, ["drug_mar"]),
                ordered_risk_from_first(df, ["mar_freq"], 1, 5),
            ]
        )
    if construct_name == "Lifestyle Recovery Deficit":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q75"], 1, 8, reverse=True),
                ordered_risk_from_first(df, ["q76"], 1, 8, reverse=True),
                ordered_risk_from_first(df, ["q80"], 1, 6),
                mapped_risk_from_first(df, ["q85"], {1: 1.0, 2: 0.75, 3: 0.45, 4: 0.05, 5: 0.0, 6: 0.25, 7: 0.55}),
                ordered_risk_from_first(df, ["q96"], 1, 6, reverse=True),
                hms_sleep_deficit(df),
                ordered_risk_from_first(df, ["exerc", "exerc_range5"], 1, 6, reverse=True),
                ordered_risk_from_first(df, ["exerc_range4"], 1, 4, reverse=True),
                ordered_risk_from_first(df, ["food_worry"], 1, 3),
            ]
        )
    if construct_name == "College Financial Strain":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["housing_worry", "housing", "housing1"], 1, 3),
                ordered_risk_from_first(df, ["food_worry"], 1, 3),
                ordered_risk_from_first(df, ["fincur"], 1, 5, reverse=True),
                ordered_risk_from_first(df, ["finpast"], 1, 5, reverse=True),
                ordered_mean_risk(df, ["afford_school", "afford_food", "afford_transp", "afford_hc", "afford_books", "afford_house"], 1, 6),
                ordered_mean_risk(df, ["pay_worry", "pay_worry1", "pay_worry2", "pay_worry3"], 1, 6, reverse=True),
            ]
        )
    if construct_name == "College Academic Adjustment":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["aca_impa", "acad_imp"], 1, 4),
                ordered_mean_risk(df, ["stress1", "stress2", "stress3", "stress4", "aca_stress"], 1, 5),
                ordered_risk_from_first(df, ["compet_sch", "compet1"], 1, 5, reverse=True),
                ordered_risk_from_first(df, ["grade_curv"], 1, 5),
                ordered_mean_risk(df, ["imposter_1", "imposter_2", "imposter_3", "imposter_4", "imposter_5"], 1, 5),
                binary_risk_from_first(df, ["failed"]),
                ordered_mean_risk(df, ["adjust_aca_1", "adjust_aca_2", "time_manage", "doubt_school_1"], 1, 6),
            ]
        )
    if construct_name == "College Belonging Deficit":
        return mean_or_null([ordered_mean_risk(df, ["belong1", "belong2", "belong8", "belong9", "belong"], 1, 6)])
    if construct_name == "College Discrimination Exposure":
        return mean_or_null([binary_any_risk(df, ["discrim_race", "discrim_culture", "discrim_gender", "discrim_sexual", "discrim_other", "discrim"])])
    if construct_name == "College Campus Safety Stress":
        return mean_or_null(
            [
                ordered_mean_risk(df, ["safe_on_day", "safe_on_night", "safe_off_day", "safe_off_night", "safe_on", "safe_off"], 1, 6),
                ordered_risk_from_first(df, ["hostcli_distress", "hostcli"], 1, 5),
            ]
        )
    if construct_name == "College Relationship Harm":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["abuse_life"], 1, 5),
                ordered_risk_from_first(df, ["abuse_recent"], 1, 5),
                binary_risk_from_first(df, ["stalk_exp", "stalk_life", "stalk_recent"]),
                ordered_risk_from_first(df, ["assault_sex", "assault_sex_y", "assault_life", "assault_recent", "sa_exp"], 1, 4),
                binary_any_risk(df, ["ipv_1", "ipv_2", "ipv_3", "ipv_4", "ipv_5", "partner_phys", "partner_insult", "partner_threat", "partner_curse"]),
            ]
        )
    if construct_name == "College Substance Exposure":
        return mean_or_null(
            [
                binary_risk_from_first(df, ["alc_any"]),
                ordered_risk_from_first(df, ["binge_fr"], 1, 6),
                binary_risk_from_first(df, ["sub_any"]),
                binary_risk_from_first(df, ["sub_cig"]),
                ordered_risk_from_first(df, ["smok_freq", "smok_vape"], 1, 5),
                binary_risk_from_first(df, ["drug_mar"]),
                ordered_risk_from_first(df, ["mar_freq"], 1, 5),
            ]
        )
    if construct_name == "College Recovery Deficit":
        return mean_or_null(
            [
                hms_sleep_deficit(df),
                ordered_risk_from_first(df, ["exerc", "exerc_range5"], 1, 6, reverse=True),
                ordered_risk_from_first(df, ["exerc_range4"], 1, 4, reverse=True),
            ]
        )
    return fast_construct_expression(df, FAST_CONSTRUCT_COLUMNS.get(construct_name, []))


def dashboard_construct_expression(df: DataFrame, construct_name: str):
    """Compact construct logic for dashboard Gold.

    The local dashboard needs construct-level signals, not every raw HMS helper column in Gold.
    These formulas keep the important cluster representatives while avoiding very large Spark expressions.
    """
    if construct_name == "Family Pressure Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q89"], 1, 5),
                ordered_risk_from_first(df, ["q90"], 1, 5),
                ordered_risk_from_first(df, ["q91"], 1, 5),
                ordered_risk_from_first(df, ["q99"], 1, 5, reverse=True),
                binary_any_risk(df, ["q100", "q101", "q102"]),
                ordered_risk_from_first(df, ["housing_worry", "housing"], 1, 3),
                ordered_risk_from_first(df, ["food_worry"], 1, 3),
                ordered_risk_from_first(df, ["fincur"], 1, 5, reverse=True),
                ordered_risk_from_first(df, ["finpast"], 1, 5, reverse=True),
            ]
        )
    if construct_name == "Academic Pressure Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q87"], 1, 5),
                ordered_risk_from_first(df, ["q103"], 1, 5),
                binary_any_risk(df, ["q105", "q106"]),
                ordered_risk_from_first(df, ["aca_impa", "acad_imp"], 1, 4),
                ordered_mean_risk(df, ["stress1", "stress2", "stress3", "stress4", "aca_stress"], 1, 5),
                binary_risk_from_first(df, ["failed"]),
            ]
        )
    if construct_name == "Peer & Safety Stress Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q14"], 1, 5),
                binary_any_risk(df, ["q18", "q24", "q25"]),
                ordered_mean_risk(df, ["belong1", "belong2", "belong8", "belong9", "belong"], 1, 6),
                binary_any_risk(df, ["discrim_race", "discrim_gender", "discrim_sexual", "discrim_other", "discrim"]),
                ordered_mean_risk(df, ["safe_on_day", "safe_on_night", "safe_off_day", "safe_off_night", "safe_on", "safe_off"], 1, 6),
            ]
        )
    if construct_name == "Trauma Exposure Index":
        return mean_or_null(
            [
                binary_risk_from_first(df, ["q19"]),
                mapped_risk_from_first(df, ["q20"], {1: 0.0, 2: 0.25, 3: 0.5, 4: 0.75, 5: 1.0}),
                mapped_risk_from_first(df, ["q21"], {1: 0.0, 2: 0.0, 3: 0.25, 4: 0.5, 5: 0.75, 6: 1.0}),
                mapped_risk_from_first(df, ["q22"], {1: 0.0, 2: 0.0, 3: 0.25, 4: 0.5, 5: 0.75, 6: 1.0}),
                binary_risk_from_first(df, ["q88"]),
                ordered_risk_from_first(df, ["abuse_life"], 1, 5),
                ordered_risk_from_first(df, ["abuse_recent"], 1, 5),
                binary_risk_from_first(df, ["stalk_exp", "stalk_life", "stalk_recent"]),
                ordered_risk_from_first(df, ["assault_sex", "sa_exp"], 1, 4),
                binary_any_risk(df, ["partner_phys", "partner_threat", "ipv_1", "ipv_2"]),
            ]
        )
    if construct_name == "Substance Coping Risk Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q33"], 1, 7),
                ordered_risk_from_first(df, ["q36"], 1, 7),
                ordered_risk_from_first(df, ["q42"], 1, 7),
                ordered_risk_from_first(df, ["q43"], 1, 6),
                ordered_risk_from_first(df, ["q48"], 1, 7),
                ordered_risk_from_first(df, ["q92"], 1, 6),
                binary_risk_from_first(df, ["alc_any"]),
                ordered_risk_from_first(df, ["binge_fr"], 1, 6),
                binary_risk_from_first(df, ["sub_any"]),
                binary_risk_from_first(df, ["sub_cig"]),
                ordered_risk_from_first(df, ["smok_freq", "smok_vape"], 1, 5),
                binary_risk_from_first(df, ["drug_mar"]),
            ]
        )
    if construct_name == "Lifestyle Recovery Deficit":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q75"], 1, 8, reverse=True),
                ordered_risk_from_first(df, ["q76"], 1, 8, reverse=True),
                ordered_risk_from_first(df, ["q80"], 1, 6),
                mapped_risk_from_first(df, ["q85"], {1: 1.0, 2: 0.75, 3: 0.45, 4: 0.05, 5: 0.0, 6: 0.25, 7: 0.55}),
                ordered_risk_from_first(df, ["q96"], 1, 6, reverse=True),
                hms_sleep_deficit(df),
                ordered_risk_from_first(df, ["exerc", "exerc_range5"], 1, 6, reverse=True),
                ordered_risk_from_first(df, ["exerc_range4"], 1, 4, reverse=True),
            ]
        )
    if construct_name == "College Financial Strain":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["housing_worry", "housing"], 1, 3),
                ordered_risk_from_first(df, ["food_worry"], 1, 3),
                ordered_risk_from_first(df, ["fincur"], 1, 5, reverse=True),
                ordered_risk_from_first(df, ["finpast"], 1, 5, reverse=True),
                ordered_risk_from_first(df, ["pay_worry"], 1, 6, reverse=True),
            ]
        )
    if construct_name == "College Academic Adjustment":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["aca_impa", "acad_imp"], 1, 4),
                ordered_mean_risk(df, ["stress1", "stress2", "stress3", "stress4", "aca_stress"], 1, 5),
                ordered_risk_from_first(df, ["compet_sch", "compet1"], 1, 5, reverse=True),
                binary_risk_from_first(df, ["failed"]),
                ordered_risk_from_first(df, ["time_manage"], 1, 6),
            ]
        )
    if construct_name == "College Belonging Deficit":
        return mean_or_null([ordered_mean_risk(df, ["belong1", "belong2", "belong8", "belong9", "belong"], 1, 6)])
    if construct_name == "College Discrimination Exposure":
        return mean_or_null([binary_any_risk(df, ["discrim_race", "discrim_gender", "discrim_sexual", "discrim_other", "discrim"])])
    if construct_name == "College Campus Safety Stress":
        return mean_or_null([ordered_mean_risk(df, ["safe_on_day", "safe_on_night", "safe_off_day", "safe_off_night", "safe_on", "safe_off"], 1, 6)])
    if construct_name == "College Relationship Harm":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["abuse_life"], 1, 5),
                ordered_risk_from_first(df, ["abuse_recent"], 1, 5),
                binary_risk_from_first(df, ["stalk_exp", "stalk_life", "stalk_recent"]),
                ordered_risk_from_first(df, ["assault_sex", "sa_exp"], 1, 4),
                binary_any_risk(df, ["partner_phys", "partner_threat", "ipv_1", "ipv_2"]),
            ]
        )
    if construct_name == "College Substance Exposure":
        return mean_or_null(
            [
                binary_risk_from_first(df, ["alc_any"]),
                ordered_risk_from_first(df, ["binge_fr"], 1, 6),
                binary_risk_from_first(df, ["sub_any"]),
                binary_risk_from_first(df, ["sub_cig"]),
                ordered_risk_from_first(df, ["smok_freq", "smok_vape"], 1, 5),
                binary_risk_from_first(df, ["drug_mar"]),
            ]
        )
    if construct_name == "College Recovery Deficit":
        return mean_or_null(
            [
                hms_sleep_deficit(df),
                ordered_risk_from_first(df, ["exerc", "exerc_range5"], 1, 6, reverse=True),
                ordered_risk_from_first(df, ["exerc_range4"], 1, 4, reverse=True),
            ]
        )
    return fast_construct_expression(df, FAST_CONSTRUCT_COLUMNS.get(construct_name, []))


def compact_dashboard_construct_expression(df: DataFrame, construct_name: str):
    """Small representative construct formulas used by the dashboard Gold table."""
    if construct_name == "Family Pressure Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q89"], 1, 5),
                ordered_risk_from_first(df, ["q90"], 1, 5),
                ordered_risk_from_first(df, ["food_worry"], 1, 3),
                ordered_risk_from_first(df, ["housing_worry", "housing"], 1, 3),
                ordered_risk_from_first(df, ["fincur"], 1, 5, reverse=True),
            ]
        )
    if construct_name == "Academic Pressure Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q87"], 1, 5),
                ordered_risk_from_first(df, ["q103"], 1, 5),
                ordered_risk_from_first(df, ["aca_impa", "acad_imp"], 1, 4),
                ordered_risk_from_first(df, ["stress1", "aca_stress"], 1, 5),
            ]
        )
    if construct_name == "Peer & Safety Stress Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q14"], 1, 5),
                binary_any_risk(df, ["q24", "q25"]),
                ordered_risk_from_first(df, ["safe_on_day", "safe_on"], 1, 6),
                ordered_risk_from_first(df, ["belong1", "belong"], 1, 6),
            ]
        )
    if construct_name == "Trauma Exposure Index":
        return mean_or_null(
            [
                binary_risk_from_first(df, ["q19"]),
                mapped_risk_from_first(df, ["q20"], {1: 0.0, 2: 0.25, 3: 0.5, 4: 0.75, 5: 1.0}),
                ordered_risk_from_first(df, ["abuse_life"], 1, 5),
                ordered_risk_from_first(df, ["assault_sex", "sa_exp"], 1, 4),
            ]
        )
    if construct_name == "Substance Coping Risk Index":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q33"], 1, 7),
                ordered_risk_from_first(df, ["q42"], 1, 7),
                binary_risk_from_first(df, ["sub_any"]),
                ordered_risk_from_first(df, ["binge_fr"], 1, 6),
            ]
        )
    if construct_name == "Lifestyle Recovery Deficit":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["q75"], 1, 8, reverse=True),
                mapped_risk_from_first(df, ["q85"], {1: 1.0, 2: 0.75, 3: 0.45, 4: 0.05, 5: 0.0, 6: 0.25, 7: 0.55}),
                hms_sleep_deficit(df),
                ordered_risk_from_first(df, ["exerc", "exerc_range5"], 1, 6, reverse=True),
            ]
        )
    if construct_name == "College Financial Strain":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["housing_worry", "housing"], 1, 3),
                ordered_risk_from_first(df, ["food_worry"], 1, 3),
                ordered_risk_from_first(df, ["fincur"], 1, 5, reverse=True),
            ]
        )
    if construct_name == "College Academic Adjustment":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["aca_impa", "acad_imp"], 1, 4),
                ordered_risk_from_first(df, ["stress1", "aca_stress"], 1, 5),
                ordered_risk_from_first(df, ["compet_sch", "compet1"], 1, 5, reverse=True),
            ]
        )
    if construct_name == "College Belonging Deficit":
        return mean_or_null([ordered_risk_from_first(df, ["belong1", "belong"], 1, 6)])
    if construct_name == "College Discrimination Exposure":
        return mean_or_null([binary_any_risk(df, ["discrim_race", "discrim_gender", "discrim_sexual", "discrim"])])
    if construct_name == "College Campus Safety Stress":
        return mean_or_null([ordered_risk_from_first(df, ["safe_on_day", "safe_on"], 1, 6)])
    if construct_name == "College Relationship Harm":
        return mean_or_null(
            [
                ordered_risk_from_first(df, ["abuse_life"], 1, 5),
                binary_risk_from_first(df, ["stalk_exp", "stalk_life"]),
                ordered_risk_from_first(df, ["assault_sex", "sa_exp"], 1, 4),
            ]
        )
    if construct_name == "College Substance Exposure":
        return mean_or_null(
            [
                binary_risk_from_first(df, ["sub_any"]),
                ordered_risk_from_first(df, ["binge_fr"], 1, 6),
                ordered_risk_from_first(df, ["smok_vape", "smok_freq"], 1, 5),
            ]
        )
    if construct_name == "College Recovery Deficit":
        return mean_or_null(
            [
                hms_sleep_deficit(df),
                ordered_risk_from_first(df, ["exerc", "exerc_range5"], 1, 6, reverse=True),
            ]
        )
    return fast_construct_expression(df, FAST_CONSTRUCT_COLUMNS.get(construct_name, []))


def build_analytic_features(df: DataFrame) -> DataFrame:
    keep_columns = [
        column
        for column in dict.fromkeys(ANALYTIC_METADATA_COLUMNS + ANALYTIC_SOURCE_COLUMNS)
        if column in df.columns
    ]
    working = with_age_group(df.select(*keep_columns))
    completeness_columns = [
        column
        for column in ["q1", "q2", "q3", "q26", "q27", "q28", "q29", "q84", "deprawsc", "anx_score", "dep_any", "sui_idea"]
        if column in working.columns and not is_gold_excluded_column(column) and not is_text_column(column)
    ]
    missing_exprs = [F.col(column).isNull().cast("int") for column in completeness_columns]
    missing_count = reduce(lambda left, right: left + right, missing_exprs) if missing_exprs else F.lit(0)
    completeness = F.round((F.lit(len(completeness_columns)) - missing_count) / F.lit(max(len(completeness_columns), 1)) * 100, 2)

    age = first_existing_numeric(working, ["age", "HMS Age", "hms_age"])
    q1 = F.coalesce(first_existing_numeric(working, ["q1"]), age_to_q1(age))
    q2 = gender_to_q2(working)
    q3 = grade_to_q3(working)
    dep_score = first_existing_numeric(working, ["deprawsc"])
    anx_score = first_existing_numeric(working, ["anx_score"])
    dep_any = first_existing_numeric(working, ["dep_any", "dep_maj"])
    sui_idea = first_existing_numeric(working, ["sui_idea"])
    sui_plan = first_existing_numeric(working, ["sui_plan"])
    sui_att = first_existing_numeric(working, ["sui_att"])
    sui_any = F.greatest(sui_idea, sui_plan, sui_att)
    q26 = F.coalesce(
        first_existing_numeric(working, ["q26"]),
        F.when((dep_any == 1) | (dep_score >= 10), 1.0).otherwise(2.0),
    )
    dep_severity = (
        F.when(dep_score.between(0, 4), 1.0)
        .when(dep_score.between(5, 9), 2.0)
        .when(dep_score.between(10, 14), 3.0)
        .when(dep_score.between(15, 19), 4.0)
        .when(dep_score >= 20, 5.0)
    )
    anx_severity = (
        F.when(anx_score.between(0, 4), 1.0)
        .when(anx_score.between(5, 9), 2.0)
        .when(anx_score.between(10, 14), 3.0)
        .when(anx_score.between(15, 19), 4.0)
        .when(anx_score >= 20, 5.0)
    )
    q84 = F.coalesce(
        first_existing_numeric(working, ["q84"]),
        F.greatest(dep_severity, anx_severity),
    )
    q27 = F.coalesce(
        first_existing_numeric(working, ["q27"]),
        F.when(sui_idea == 1, 1.0).when(sui_idea.isNotNull(), 2.0),
    )
    q28 = F.coalesce(
        first_existing_numeric(working, ["q28"]),
        F.when(sui_plan == 1, 1.0).when(sui_plan.isNotNull(), 2.0),
    )
    q29 = F.coalesce(
        first_existing_numeric(working, ["q29"]),
        F.when(sui_att == 1, 1.0).when(sui_att.isNotNull(), 2.0),
    )
    mental_risk_flag = (
        (q26 == 1)
        | (q84 >= 4)
        | (dep_any == 1)
        | (dep_score >= 10)
        | (anx_score >= 10)
        | (sui_any == 1)
    )
    risk_flag = mental_risk_flag
    high_score_flag = (
        (q84 >= 4)
        | (dep_score >= 10)
        | (anx_score >= 10)
    )
    target = F.when(mental_risk_flag, 1).otherwise(0)

    selected_columns = [column for column in ANALYTIC_METADATA_COLUMNS if column in working.columns]
    passthrough_columns = [
        column
        for column in ANALYTIC_OUTPUT_PASSTHROUGH_COLUMNS
        if column in working.columns
        and column not in set(selected_columns)
        and column not in {"q1", "q2", "q3", "q26", "q27", "q28", "q29", "q84"}
    ]
    output_expressions = [
        *[F.col(column) for column in selected_columns],
        data_source_expr(working).alias(DATA_SOURCE_COLUMN),
        population_expr().alias(POPULATION_COLUMN),
        F.coalesce(first_existing_numeric(working, ["inst_hmsyear"]).cast("string"), data_source_expr(working)).alias(STUDY_YEAR_COLUMN),
        F.when(F.col("source_group") == "university", age).alias(HMS_AGE_COLUMN),
        q1.alias("q1"),
        q2.alias("q2"),
        q3.alias("q3"),
        q26.alias("q26"),
        q27.alias("q27"),
        q28.alias("q28"),
        q29.alias("q29"),
        q84.alias("q84"),
        target.alias("Target"),
        *[F.col(column) for column in passthrough_columns],
        *[
            compact_dashboard_construct_expression(working, construct_name).alias(construct_name)
            for construct_name in RESEARCH_FEATURES + HMS_NATIVE_FEATURES
        ],
        F.col("age_group"),
        F.col("source_group").alias("school_or_university"),
        risk_flag.alias("risk_related_flag"),
        high_score_flag.alias("high_score_flag"),
        missing_count.alias("missing_field_count"),
        completeness.alias("answer_completeness_rate"),
        F.current_timestamp().alias("gold_processed_at"),
    ]
    return working.select(*output_expressions)


def output_table(
    df: Optional[DataFrame],
    path: str,
    table_name: str,
    write_mode: str,
    max_small_table_rows: int,
    output_partitions: int,
    known_rows: Optional[int] = None,
) -> Dict[str, object]:
    if df is None:
        print(f"WARNING: Skip {table_name}; DataFrame was not created.")
        return {"rows": 0, "path": path, "skipped": True}
    for field in df.schema.fields:
        if isinstance(field.dataType, T.NullType):
            df = df.withColumn(field.name, F.lit(None).cast("string"))
    print(f"\nGOLD TABLE: {table_name}")
    df.printSchema()
    if table_name == "survey_analytic_features" and known_rows is not None:
        rows = known_rows
        print(f"{table_name} rows: {rows} (same as Silver input; skipped separate count action)")
        print(f"Writing Gold Parquet to {path} with mode={write_mode}")
        # This is the heaviest record-level Gold table. Write it once, directly, to avoid a second
        # full Spark action before the Parquet write. Do not repartition here: for the current
        # dashboard-sized survey data, avoiding shuffle is faster than forcing a new file layout.
        df.write.mode(write_mode).parquet(path)
        return {"rows": rows, "path": path, "skipped": False}

    cached = df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        rows = cached.count()
        print(f"{table_name} rows: {rows}")
        print(f"Writing Gold Parquet to {path} with mode={write_mode}")
        if table_name == "survey_analytic_features":
            # Analytic features are record-level but already compacted to dashboard-needed columns only.
            # Coalesce avoids an extra shuffle before writing a bounded number of Parquet parts to GCS.
            cached.coalesce(max(1, output_partitions)).write.mode(write_mode).parquet(path)
        else:
            if rows <= max_small_table_rows:
                # Gold aggregate tables are small dashboard-ready outputs; one part makes dashboard loading simpler.
                # Do not use coalesce(1) for Silver or large record-level datasets.
                cached.coalesce(1).write.mode(write_mode).parquet(path)
            else:
                cached.coalesce(max(1, output_partitions)).write.mode(write_mode).parquet(path)
        return {"rows": rows, "path": path, "skipped": False}
    finally:
        cached.unpersist()


def cast_nulltype_columns(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, T.NullType):
            df = df.withColumn(field.name, F.lit(None).cast("string"))
    return df


def filter_process_date(df: DataFrame, process_date: Optional[str]) -> DataFrame:
    if not process_date:
        return df
    target_date = F.to_date(F.lit(process_date))
    if "date" in df.columns:
        print(f"Filtering Silver survey by date={process_date}")
        return df.where(F.col("date").cast("date") == target_date)
    if "ingestion_date" in df.columns:
        print(f"Filtering Silver survey by ingestion_date={process_date}")
        return df.where(F.col("ingestion_date").cast("date") == target_date)
    print("WARNING: --process-date was provided, but Silver has no date or ingestion_date column; no date filter applied.")
    return df


def prune_silver_for_gold(df: DataFrame) -> DataFrame:
    required_columns = list(
        dict.fromkeys(
            ANALYTIC_METADATA_COLUMNS
            + ANALYTIC_SOURCE_COLUMNS
            + DASHBOARD_QUESTION_DISTRIBUTION_COLUMNS
            + DASHBOARD_NUMERIC_SUMMARY_COLUMNS
            + [
                "source_file",
                "source_group",
                "source_dataset",
                "date",
                "ingestion_date",
                "is_valid",
                "gender",
                "sex",
                "age",
                "grade",
            ]
        )
    )
    keep_columns = [column for column in required_columns if column in df.columns]
    if not keep_columns:
        print("WARNING: Gold input pruning found no matching dashboard columns; keeping original Silver schema.")
        return df
    missing_count = len([column for column in required_columns if column not in df.columns])
    print(f"Pruning Silver columns for Gold dashboard: {len(df.columns)} -> {len(keep_columns)} columns.")
    print(f"Dashboard-required columns missing in this run: {missing_count}")
    return df.select(*keep_columns)


def build_compact_silver_input(df: DataFrame, process_date: Optional[str]) -> DataFrame:
    compact = prune_silver_for_gold(ensure_source_group(df))
    return filter_process_date(compact, process_date)


def stage_compact_silver(
    spark: SparkSession,
    raw_silver: DataFrame,
    process_date: Optional[str],
    temp_path: str,
    write_mode: str,
    output_partitions: int,
) -> DataFrame:
    compact = cast_nulltype_columns(build_compact_silver_input(raw_silver, process_date))
    partitions = max(1, output_partitions)
    print("\nSTAGE A: Silver -> compact temp Parquet")
    print(f"Temp compact Silver path: {temp_path}")
    print(f"Temp write mode: {write_mode}")
    print(f"Temp output partitions: {partitions}")
    compact.printSchema()
    # This materializes only the pruned dashboard columns. Stage B reads this compact Parquet,
    # so construct/index calculation no longer carries the original wide Silver read lineage.
    compact.repartition(partitions).write.mode(write_mode).parquet(temp_path)
    print("Stage A completed. Reading compact temp Parquet for Stage B.")
    return spark.read.parquet(temp_path)


def main() -> None:
    args = parse_args()
    start_time = time.time()
    run_id = args.run_id or default_run_id()
    versioned_output = not args.disable_versioned_output
    write_mode = effective_write_mode(args.write_mode, versioned_output)
    output_paths = {
        "survey_analytic_features": versioned_path(args.analytic_features_output_path, run_id, versioned_output),
        "survey_overview_summary": versioned_path(args.overview_output_path, run_id, versioned_output),
        "survey_response_by_date": versioned_path(args.response_by_date_output_path, run_id, versioned_output),
        "survey_demographic_summary": versioned_path(args.demographic_output_path, run_id, versioned_output),
        "survey_question_distribution": versioned_path(args.question_distribution_output_path, run_id, versioned_output),
        "survey_numeric_summary": versioned_path(args.numeric_summary_output_path, run_id, versioned_output),
    }
    temp_compact_path = compact_temp_path(args.temp_work_path, run_id)
    spark = (
        SparkSession.builder.appName("survey-silver-to-gold-dashboard-tables")
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.default.parallelism", "48")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Reading Silver input only: {args.input_path}")
        try:
            silver = spark.read.parquet(args.input_path)
        except AnalysisException as exc:
            print(
                "ERROR: Silver Parquet input is not available. "
                "Run survey_bronze_to_silver_spark.py first, then run Silver -> Gold."
            )
            raise exc

        if args.disable_temp_stage:
            print("WARNING: --disable-temp-stage enabled; Stage A compact Parquet will be skipped.")
            silver = build_compact_silver_input(silver, args.process_date)
            temp_stage_enabled = False
            temp_path_for_log = None
        else:
            silver = stage_compact_silver(
                spark,
                silver,
                args.process_date,
                temp_compact_path,
                write_mode,
                args.temp_output_partitions,
            )
            temp_stage_enabled = True
            temp_path_for_log = temp_compact_path

        # Reused by six Gold builders; MEMORY_AND_DISK avoids recomputing the compact dashboard input.
        silver = silver.persist(StorageLevel.MEMORY_AND_DISK)
        silver.printSchema()
        input_rows = silver.count()
        print(f"Silver rows available for Gold creation: {input_rows}")

        output_rows = {
            "survey_analytic_features": output_table(
                build_analytic_features(silver),
                output_paths["survey_analytic_features"],
                "survey_analytic_features",
                write_mode,
                args.gold_max_small_table_rows,
                args.gold_output_partitions,
                known_rows=input_rows,
            ),
            "survey_overview_summary": output_table(
                build_overview(silver),
                output_paths["survey_overview_summary"],
                "survey_overview_summary",
                write_mode,
                args.gold_max_small_table_rows,
                args.gold_output_partitions,
            ),
            "survey_response_by_date": output_table(
                build_response_by_date(silver),
                output_paths["survey_response_by_date"],
                "survey_response_by_date",
                write_mode,
                args.gold_max_small_table_rows,
                args.gold_output_partitions,
            ),
            "survey_demographic_summary": output_table(
                build_demographic_summary(silver),
                output_paths["survey_demographic_summary"],
                "survey_demographic_summary",
                write_mode,
                args.gold_max_small_table_rows,
                args.gold_output_partitions,
            ),
            "survey_question_distribution": output_table(
                build_question_distribution(silver),
                output_paths["survey_question_distribution"],
                "survey_question_distribution",
                write_mode,
                args.gold_max_small_table_rows,
                args.gold_output_partitions,
            ),
            "survey_numeric_summary": output_table(
                build_numeric_summary(silver),
                output_paths["survey_numeric_summary"],
                "survey_numeric_summary",
                write_mode,
                args.gold_max_small_table_rows,
                args.gold_output_partitions,
            ),
        }
        print("Gold output completed. Every Gold table was created from Silver input only.")
        print_json_log(
            {
                "job_name": "survey_silver_to_gold",
                "project_id": PROJECT_ID,
                "input_path": args.input_path,
                "process_date": args.process_date,
                "requested_write_mode": args.write_mode,
                "effective_write_mode": write_mode,
                "versioned_output": versioned_output,
                "run_id": run_id,
                "temp_stage_enabled": temp_stage_enabled,
                "temp_compact_path": temp_path_for_log,
                "input_rows": input_rows,
                "output_rows": output_rows,
                "output_paths": output_paths,
                "output_success": True,
                "duration_seconds": round(time.time() - start_time, 2),
                "status": "success",
            }
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
