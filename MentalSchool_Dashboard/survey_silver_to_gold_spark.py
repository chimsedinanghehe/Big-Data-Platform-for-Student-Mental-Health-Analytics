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
DISCRIMINATION_COLUMNS = [
    "discrim_race",
    "discrim_culture",
    "discrim_gender",
    "discrim_sexual",
    "discrim_other",
    "discrim",
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
    *DISCRIMINATION_COLUMNS,
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
    *DISCRIMINATION_COLUMNS,
    "safe_on",
    "abuse_life",
    "sub_any",
    "sleep_wknight",
    "exerc",
]
FAST_CONSTRUCT_COLUMNS = {
    "Family Pressure Index": ["q89", "food_worry", "housing_worry", "fincur", "pay_worry"],
    "Academic Pressure Index": ["q87", "q103", "aca_impa", "stress1", "failed"],
    "Peer & Safety Stress Index": ["q14", "belong1", "safe_on_day", *DISCRIMINATION_COLUMNS],
    "Trauma Exposure Index": ["q19", "abuse_life", "stalk_exp", "assault_sex", "sa_exp", "partner_phys", "ipv_1"],
    "Substance Coping Risk Index": ["q33", "sub_any", "binge_fr", "smok_vape", "smok_freq"],
    "Lifestyle Recovery Deficit": ["q75", "sleep_wknight", "exerc", "exerc_range5"],
    "College Financial Strain": ["fincur", "food_worry", "housing_worry", "pay_worry"],
    "College Academic Adjustment": ["aca_impa", "stress1", "compet_sch", "failed", "time_manage"],
    "College Belonging Deficit": ["belong1"],
    "College Discrimination Exposure": DISCRIMINATION_COLUMNS,
    "College Campus Safety Stress": ["safe_on_day", "hostcli_distress"],
    "College Relationship Harm": ["abuse_life", "stalk_exp", "assault_sex", "sa_exp", "partner_phys", "ipv_1"],
    "College Substance Exposure": ["sub_any", "binge_fr", "smok_vape", "smok_freq"],
    "College Recovery Deficit": ["sleep_wknight", "exerc", "exerc_range5"],
}
FAST_VALUE_RANGES = {
    "q14": (1.0, 5.0, False),
    "q33": (1.0, 7.0, False),
    "q75": (1.0, 8.0, True),
    "q87": (1.0, 5.0, False),
    "q89": (1.0, 5.0, False),
    "fincur": (1.0, 5.0, True),
    "food_worry": (1.0, 3.0, False),
    "housing_worry": (1.0, 3.0, False),
    "pay_worry": (1.0, 6.0, True),
    "aca_impa": (1.0, 4.0, False),
    "aca_stress": (1.0, 5.0, False),
    "stress1": (1.0, 5.0, False),
    "compet_sch": (1.0, 5.0, True),
    "time_manage": (1.0, 6.0, False),
    "belong": (1.0, 6.0, False),
    "belong1": (1.0, 6.0, False),
    "safe_on": (1.0, 6.0, False),
    "safe_on_day": (1.0, 6.0, False),
    "hostcli_distress": (1.0, 5.0, False),
    "abuse_life": (1.0, 5.0, False),
    "binge_fr": (1.0, 6.0, False),
    "smok_vape": (1.0, 5.0, False),
    "smok_freq": (1.0, 5.0, False),
    "sleep_wknight": (4.0, 10.0, True),
    "exerc": (1.0, 6.0, True),
    "exerc_range5": (1.0, 6.0, True),
}
FAST_BINARY_RISK_COLUMNS = {
    "q19",
    "failed",
    "sub_any",
    "stalk_exp",
    "partner_phys",
    "ipv_1",
    *DISCRIMINATION_COLUMNS,
}
FIXED_CONSTRUCT_COLUMNS = {
    "Family Pressure Index": ["q89", "q90", "q91", "q99", "q100", "q101", "q102", "q104"],
    "Academic Pressure Index": ["q87", "q103", "q105", "q106"],
    "Peer & Safety Stress Index": ["q14", "q15", "q18", "q24", "q25", *DISCRIMINATION_COLUMNS],
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
    "College Discrimination Exposure": DISCRIMINATION_COLUMNS,
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
            *DISCRIMINATION_COLUMNS,
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
DEFAULT_QUESTION_DISTRIBUTION_COLUMNS = list(
    dict.fromkeys(
        [
            "q1",
            "q2",
            "q3",
            "q14",
            "q19",
            "q24",
            "q25",
            "q26",
            "q27",
            "q28",
            "q29",
            "q33",
            "q42",
            "q75",
            "q76",
            "q84",
            "q87",
            "q89",
            "q103",
            "fincur",
            "food_worry",
            "aca_impa",
            "aca_stress",
            "belong",
            *DISCRIMINATION_COLUMNS,
            "safe_on",
            "abuse_life",
            "sub_any",
            "sleep_wknight",
            "exerc",
        ]
    )
)
DEFAULT_NUMERIC_SUMMARY_COLUMNS = list(
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
            "answer_completeness_rate",
        ]
    )
)
TABLE_ALIASES = {
    "analytic_features": "survey_analytic_features",
    "overview": "survey_overview_summary",
    "response_by_date": "survey_response_by_date",
    "demographic": "survey_demographic_summary",
    "question_distribution": "survey_question_distribution",
    "numeric_summary": "survey_numeric_summary",
}
CORE_TABLES = ["analytic_features", "overview", "response_by_date", "demographic"]
HEAVY_TABLES = ["question_distribution", "numeric_summary"]
ALL_TABLES = [*CORE_TABLES, *HEAVY_TABLES]


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
    parser.add_argument("--gold-output-partitions", type=int, default=4)
    parser.add_argument(
        "--analytic-compute-partitions",
        type=int,
        default=0,
        help="Optional repartition count before computing survey_analytic_features. Use 0 to keep natural Silver file partitions.",
    )
    parser.add_argument("--spark-parallelism", type=int, default=8)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--temp-work-path", default=TEMP_SURVEY_GOLD_WORK_PATH)
    parser.add_argument("--temp-output-partitions", type=int, default=4)
    parser.add_argument(
        "--tables",
        default="all",
        help=(
            "Comma-separated Gold tables to build. Use all, core, heavy, or names: "
            "analytic_features,overview,response_by_date,demographic,question_distribution,numeric_summary."
        ),
    )
    parser.add_argument("--enable-output-verify", action="store_true", help="Count rows before/after writes for audit logs.")
    parser.add_argument("--enable-counts", action="store_true", help="Alias for --enable-output-verify.")
    parser.add_argument("--enable-schema-report", action="store_true", help="Print schemas for debug only.")
    parser.add_argument("--enable-temp-stage", action="store_true", help="Materialize compact Silver temp Parquet before Gold builders.")
    parser.add_argument("--cache-silver", action="store_true", help="Persist compact Silver between table writes. Off by default for small compact inputs.")
    parser.add_argument(
        "--disable-temp-stage",
        action="store_true",
        help="Skip Stage A compact temp Parquet. This is the production default unless --enable-temp-stage is set.",
    )
    parser.add_argument("--question-distribution-columns", help="Comma-separated columns for survey_question_distribution.")
    parser.add_argument("--full-question-distribution", action="store_true", help="Use all dashboard question distribution columns.")
    parser.add_argument("--numeric-summary-columns", help="Comma-separated columns for survey_numeric_summary.")
    parser.add_argument("--full-numeric-summary", action="store_true", help="Use all dashboard numeric summary columns.")
    parser.add_argument(
        "--analytic-construct-mode",
        default="fast",
        choices=["compact", "fast", "semantic"],
        help="Construct formula set for survey_analytic_features. fast is production refresh; compact/semantic are deeper audit modes.",
    )
    parser.add_argument("--enable-wholestage-codegen", action="store_true", help="Enable Spark whole-stage codegen for benchmark runs.")
    parser.add_argument(
        "--disable-wholestage-codegen",
        action="store_true",
        help="Keep Spark whole-stage codegen disabled. This is the production default for the wide construct expressions.",
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


def parse_csv_columns(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_requested_tables(value: str) -> List[str]:
    requested: List[str] = []
    tokens = parse_csv_columns(value or "all")
    for token in tokens:
        lowered = token.lower()
        if lowered == "all":
            requested.extend(ALL_TABLES)
        elif lowered == "core":
            requested.extend(CORE_TABLES)
        elif lowered == "heavy":
            requested.extend(HEAVY_TABLES)
        elif lowered in TABLE_ALIASES:
            requested.append(lowered)
        elif lowered in TABLE_ALIASES.values():
            requested.extend([name for name, table in TABLE_ALIASES.items() if table == lowered])
        else:
            raise ValueError(f"Unknown Gold table selector: {token}")
    return [table for table in ALL_TABLES if table in set(requested)]


def quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def stack_unpivot(df: DataFrame, columns: Sequence[str], value_cast: str, name_col: str, value_col: str) -> Optional[DataFrame]:
    existing = [column for column in dict.fromkeys(columns) if column in df.columns]
    if not existing:
        return None
    stack_args = ", ".join(
        f"'{column}', CAST({quote_identifier(column)} AS {value_cast})"
        for column in existing
    )
    return df.selectExpr(
        "source_group",
        f"stack({len(existing)}, {stack_args}) as ({quote_identifier(name_col)}, {quote_identifier(value_col)})",
    )


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


def applicable_payload_columns(df: DataFrame, payload: List[str], source_group: str) -> List[str]:
    payload_set = set(payload)
    if source_group == "school":
        candidates = [*SCHOOL_ANALYTIC_COLUMNS, "age", "gender", "grade"]
    elif source_group == "university":
        candidates = [*HMS_ANALYTIC_COLUMNS, "age", "gender", "sex", "yr_sch", "inst_hmsyear"]
    else:
        candidates = payload
    return [column for column in dict.fromkeys(candidates) if column in df.columns and column in payload_set]


def group_quality_counts(df: DataFrame, source_group: str, columns: List[str]) -> Dict[str, int]:
    if source_group == "__other__":
        filtered = df.where(~F.col("source_group").isin("school", "university"))
    else:
        filtered = df.where(F.col("source_group") == F.lit(source_group))
    if not columns:
        row = filtered.agg(F.count(F.lit(1)).alias("rows")).first().asDict()
        return {"rows": int(row.get("rows") or 0), "columns": 0, "non_null_cells": 0}

    row = (
        filtered.agg(
            F.count(F.lit(1)).alias("rows"),
            *[F.count(F.col(column)).alias(f"__nn_{idx}") for idx, column in enumerate(columns)],
        )
        .first()
        .asDict()
    )
    rows = int(row.get("rows") or 0)
    non_null_cells = sum(int(row.get(f"__nn_{idx}") or 0) for idx, _ in enumerate(columns))
    return {"rows": rows, "columns": len(columns), "non_null_cells": non_null_cells}


def build_overview(df: DataFrame) -> DataFrame:
    payload = payload_columns(df)
    total_cells = len(payload)

    def non_null_count_expr(columns: List[str]):
        if not columns:
            return F.lit(0)
        return reduce(lambda left, right: left + right, [F.col(column).isNotNull().cast("long") for column in columns])

    school_columns = applicable_payload_columns(df, payload, "school")
    university_columns = applicable_payload_columns(df, payload, "university")
    other_columns = applicable_payload_columns(df, payload, "unknown")
    source_group = F.col("source_group")
    applicable_cells = (
        F.when(source_group == "school", F.lit(len(school_columns)))
        .when(source_group == "university", F.lit(len(university_columns)))
        .otherwise(F.lit(len(other_columns)))
    )
    non_null_cells = (
        F.when(source_group == "school", non_null_count_expr(school_columns))
        .when(source_group == "university", non_null_count_expr(university_columns))
        .otherwise(non_null_count_expr(other_columns))
    )
    aggregate = df.agg(
        F.count(F.lit(1)).alias("total_responses"),
        F.sum(F.when(valid_condition(df), 1).otherwise(0)).alias("valid_responses"),
        F.sum(F.when(source_group == "school", 1).otherwise(0)).alias("school_responses"),
        F.sum(F.when(source_group == "university", 1).otherwise(0)).alias("university_responses"),
        F.sum(applicable_cells.cast("long")).alias("__applicable_cells"),
        F.sum(non_null_cells.cast("long")).alias("__non_null_cells"),
    )
    return aggregate.select(
        "total_responses",
        F.lit(total_cells).alias("total_columns"),
        "valid_responses",
        (F.col("total_responses") - F.col("valid_responses")).alias("invalid_responses"),
        F.when(
            F.col("__applicable_cells") > 0,
            F.round((F.col("__applicable_cells") - F.col("__non_null_cells")) / F.col("__applicable_cells") * 100, 2),
        )
        .otherwise(F.lit(0.0))
        .alias("missing_value_rate"),
        "school_responses",
        "university_responses",
        F.current_timestamp().alias("processed_at"),
    )


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
    unpivoted = stack_unpivot(df, list(columns), "string", name_col, value_col)
    if unpivoted is None:
        return None
    dimension_window = Window.partitionBy(name_col)
    return (
        unpivoted.where(F.col(value_col).isNotNull())
        .groupBy(name_col, value_col)
        .agg(F.count(F.lit(1)).alias("count"))
        .withColumn("__dimension_total", F.sum("count").over(dimension_window))
        .withColumn("percentage", F.round(F.col("count") / F.col("__dimension_total") * 100, 2))
        .select(name_col, value_col, "count", "percentage")
    )


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


def build_question_distribution(
    df: DataFrame,
    *,
    selected_columns: Optional[Sequence[str]] = None,
    full_distribution: bool = False,
) -> Optional[DataFrame]:
    base_columns = (
        DASHBOARD_QUESTION_DISTRIBUTION_COLUMNS
        if full_distribution
        else (list(selected_columns or []) or DEFAULT_QUESTION_DISTRIBUTION_COLUMNS)
    )
    columns = [column for column in base_columns if column in df.columns]
    if not columns:
        columns = sorted(set(string_categorical_columns(df) + numeric_question_columns(df)))[:60]
    if not columns:
        print("WARNING: Skip survey_question_distribution because no categorical/question columns were found.")
        return None

    unpivoted = stack_unpivot(df, columns, "string", "column_name", "answer_value")
    if unpivoted is None:
        return None
    source_column_window = Window.partitionBy("source_group", "column_name")
    return (
        unpivoted.where(F.col("answer_value").isNotNull())
        .groupBy("source_group", "column_name", "answer_value")
        .agg(F.count(F.lit(1)).alias("count"))
        .withColumn("__column_total", F.sum("count").over(source_column_window))
        .withColumn("percentage", F.round(F.col("count") / F.col("__column_total") * 100, 2))
        .select("column_name", "answer_value", "source_group", "count", "percentage")
    )


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


def build_numeric_summary(
    df: DataFrame,
    *,
    selected_columns: Optional[Sequence[str]] = None,
    full_summary: bool = False,
) -> Optional[DataFrame]:
    base_columns = (
        DASHBOARD_NUMERIC_SUMMARY_COLUMNS
        if full_summary
        else (list(selected_columns or []) or DEFAULT_NUMERIC_SUMMARY_COLUMNS)
    )
    columns = [column for column in base_columns if column in df.columns]
    if not columns:
        columns = numeric_columns(df)[:80]
    if not columns:
        print("WARNING: Skip survey_numeric_summary because no numeric columns were found.")
        return None

    unpivoted = stack_unpivot(df, columns, "double", "column_name", "numeric_value")
    if unpivoted is None:
        return None
    return (
        unpivoted.groupBy("source_group", "column_name")
        .agg(
            F.count("numeric_value").alias("count"),
            F.avg("numeric_value").alias("avg"),
            F.min("numeric_value").alias("min"),
            F.max("numeric_value").alias("max"),
            F.stddev("numeric_value").alias("stddev"),
        )
        .select("column_name", "source_group", "count", "avg", "min", "max", "stddev")
    )


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
    if column in FAST_BINARY_RISK_COLUMNS:
        text = F.lower(F.trim(F.col(column).cast("string")))
        return (
            F.when(value == 1, F.lit(1.0))
            .when(value.isin(0, 2), F.lit(0.0))
            .when(text.isin("yes", "y", "true", "t"), F.lit(1.0))
            .when(text.isin("no", "n", "false", "f"), F.lit(0.0))
        )
    min_value, max_value, reverse = FAST_VALUE_RANGES.get(column, (1.0, 5.0, False))
    if column == "sleep_wknight":
        unit = F.when(value.isNotNull(), F.abs(value - F.lit(8.0)) / F.lit(4.0))
        return clamp_unit_interval(unit)
    unit = F.when(value.between(min_value, max_value), (value - F.lit(min_value)) / F.lit(max_value - min_value))
    if reverse:
        unit = F.lit(1.0) - unit
    return clamp_unit_interval(unit)


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
                binary_any_risk(df, DISCRIMINATION_COLUMNS),
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
        return mean_or_null([binary_any_risk(df, DISCRIMINATION_COLUMNS)])
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
                binary_any_risk(df, DISCRIMINATION_COLUMNS),
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
        return mean_or_null([binary_any_risk(df, DISCRIMINATION_COLUMNS)])
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
                binary_any_risk(df, DISCRIMINATION_COLUMNS),
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
        return mean_or_null([binary_any_risk(df, DISCRIMINATION_COLUMNS)])
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


def construct_expression_for_mode(df: DataFrame, construct_name: str, mode: str):
    if mode == "fast":
        return fast_construct_expression(df, FAST_CONSTRUCT_COLUMNS.get(construct_name, []))
    if mode == "semantic":
        return semantic_construct_expression(df, construct_name)
    return compact_dashboard_construct_expression(df, construct_name)


def build_analytic_features(df: DataFrame, analytic_partitions: int = 0, construct_mode: str = "compact") -> DataFrame:
    keep_columns = [
        column
        for column in dict.fromkeys(ANALYTIC_METADATA_COLUMNS + ANALYTIC_SOURCE_COLUMNS)
        if column in df.columns
    ]
    working = with_age_group(df.select(*keep_columns))
    if analytic_partitions > 0:
        # The construct projection is CPU-heavy. Repartition before building expressions so
        # Spark can use the executor cores instead of evaluating the whole projection in a few
        # combined file-scan tasks.
        working = working.repartition(analytic_partitions)
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
            construct_expression_for_mode(working, construct_name, construct_mode).alias(construct_name)
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
    count_enabled: bool = False,
    schema_report_enabled: bool = False,
) -> Dict[str, object]:
    if df is None:
        print(f"WARNING: Skip {table_name}; DataFrame was not created.")
        return {"rows": 0, "path": path, "skipped": True}
    for field in df.schema.fields:
        if isinstance(field.dataType, T.NullType):
            df = df.withColumn(field.name, F.lit(None).cast("string"))
    print(f"\nGOLD TABLE: {table_name}")
    if schema_report_enabled:
        df.printSchema()
    if known_rows is not None:
        rows = known_rows
        print(f"{table_name} rows: {rows} (known from input count)")
        print(f"Writing Gold Parquet to {path} with mode={write_mode}")
        writer_df = df.coalesce(max(1, output_partitions)) if table_name == "survey_analytic_features" else df.coalesce(1)
        writer_df.write.mode(write_mode).parquet(path)
        return {"rows": rows, "path": path, "skipped": False}

    if not count_enabled:
        print(f"{table_name} rows: not_counted_for_speed")
        print(f"Writing Gold Parquet to {path} with mode={write_mode}")
        writer_df = df.coalesce(max(1, output_partitions)) if table_name == "survey_analytic_features" else df.coalesce(1)
        writer_df.write.mode(write_mode).parquet(path)
        return {"rows": "not_counted_for_speed", "path": path, "skipped": False}

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
    schema_report_enabled: bool = False,
) -> DataFrame:
    compact = cast_nulltype_columns(build_compact_silver_input(raw_silver, process_date))
    partitions = max(1, output_partitions)
    print("\nSTAGE A: Silver -> compact temp Parquet")
    print(f"Temp compact Silver path: {temp_path}")
    print(f"Temp write mode: {write_mode}")
    print(f"Temp output partitions: {partitions}")
    if schema_report_enabled:
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
    requested_tables = parse_requested_tables(args.tables)
    count_enabled = bool(args.enable_output_verify or args.enable_counts)
    temp_stage_requested = bool(args.enable_temp_stage and not args.disable_temp_stage)
    whole_stage_codegen_enabled = bool(args.enable_wholestage_codegen and not args.disable_wholestage_codegen)
    spark_parallelism = max(1, args.spark_parallelism)
    shuffle_partitions = max(1, args.shuffle_partitions)
    gold_output_partitions = max(1, args.gold_output_partitions)
    analytic_compute_partitions = max(0, args.analytic_compute_partitions)
    temp_output_partitions = max(1, args.temp_output_partitions)
    stage_durations: Dict[str, float] = {}
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
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(spark_parallelism))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.codegen.wholeStage", "true" if whole_stage_codegen_enabled else "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Reading Silver input only: {args.input_path}")
        read_start = time.time()
        try:
            silver = spark.read.parquet(args.input_path)
        except AnalysisException as exc:
            print(
                "ERROR: Silver Parquet input is not available. "
                "Run survey_bronze_to_silver_spark.py first, then run Silver -> Gold."
            )
            raise exc
        stage_durations["read_silver_seconds"] = round(time.time() - read_start, 2)

        temp_start = time.time()
        if not temp_stage_requested:
            print("Skipping compact temp Parquet stage in production mode.")
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
                temp_output_partitions,
                args.enable_schema_report,
            )
            temp_stage_enabled = True
            temp_path_for_log = temp_compact_path
        stage_durations["temp_stage_seconds"] = round(time.time() - temp_start, 2)

        if args.enable_schema_report:
            silver.printSchema()
        if args.cache_silver and len(requested_tables) > 1:
            silver = silver.persist(StorageLevel.MEMORY_AND_DISK)
            silver_cached = True
        else:
            silver_cached = False

        input_rows = None
        if count_enabled:
            count_start = time.time()
            input_rows = silver.count()
            stage_durations["input_count_seconds"] = round(time.time() - count_start, 2)
            print(f"Silver rows available for Gold creation: {input_rows}")
        else:
            print("Silver rows available for Gold creation: not_counted_for_speed")

        question_distribution_columns = parse_csv_columns(args.question_distribution_columns)
        numeric_summary_columns = parse_csv_columns(args.numeric_summary_columns)
        table_builders = {
            "analytic_features": lambda: build_analytic_features(
                silver,
                analytic_partitions=analytic_compute_partitions,
                construct_mode=args.analytic_construct_mode,
            ),
            "overview": lambda: build_overview(silver),
            "response_by_date": lambda: build_response_by_date(silver),
            "demographic": lambda: build_demographic_summary(silver),
            "question_distribution": lambda: build_question_distribution(
                silver,
                selected_columns=question_distribution_columns,
                full_distribution=args.full_question_distribution,
            ),
            "numeric_summary": lambda: build_numeric_summary(
                silver,
                selected_columns=numeric_summary_columns,
                full_summary=args.full_numeric_summary,
            ),
        }
        output_rows: Dict[str, object] = {}
        tables_written: List[str] = []
        try:
            for table_key in requested_tables:
                internal_name = TABLE_ALIASES[table_key]
                table_start = time.time()
                output_rows[internal_name] = output_table(
                    table_builders[table_key](),
                    output_paths[internal_name],
                    internal_name,
                    write_mode,
                    args.gold_max_small_table_rows,
                    gold_output_partitions,
                    known_rows=input_rows if table_key == "analytic_features" and input_rows is not None else None,
                    count_enabled=count_enabled,
                    schema_report_enabled=args.enable_schema_report,
                )
                stage_durations[f"{table_key}_seconds"] = round(time.time() - table_start, 2)
                if not output_rows[internal_name].get("skipped"):
                    tables_written.append(table_key)
        finally:
            if silver_cached:
                silver.unpersist()

        skipped_tables = [table for table in ALL_TABLES if table not in requested_tables]
        print("Gold output completed. Requested Gold tables were created from Silver input only.")
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
                "tables_requested": requested_tables,
                "tables_written": tables_written,
                "skipped_tables": skipped_tables,
                "partition_config": {
                    "spark_default_parallelism": spark_parallelism,
                    "spark_shuffle_partitions": shuffle_partitions,
                    "gold_output_partitions": gold_output_partitions,
                    "analytic_compute_partitions": analytic_compute_partitions,
                    "temp_output_partitions": temp_output_partitions,
                    "adaptive_enabled": True,
                    "adaptive_coalesce_partitions_enabled": True,
                    "whole_stage_codegen_enabled": whole_stage_codegen_enabled,
                },
                "temp_stage_enabled": temp_stage_enabled,
                "temp_compact_path": temp_path_for_log,
                "cache_silver_enabled": silver_cached,
                "analytic_construct_mode": args.analytic_construct_mode,
                "count_enabled": count_enabled,
                "schema_report_enabled": args.enable_schema_report,
                "input_rows": input_rows if input_rows is not None else "not_counted_for_speed",
                "output_rows": output_rows,
                "output_paths": {TABLE_ALIASES[key]: output_paths[TABLE_ALIASES[key]] for key in requested_tables},
                "output_success": True,
                "stage_durations": stage_durations,
                "duration_seconds": round(time.time() - start_time, 2),
                "status": "success",
            }
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
