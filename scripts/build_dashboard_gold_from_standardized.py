from __future__ import annotations

import argparse
import sys
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_ROOT = PROJECT_ROOT / "MentalSchool_Dashboard"
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from utils.dashboard_core import (  # noqa: E402
    DATA_SOURCE_COLUMN,
    HMS_POPULATION_LABEL,
    INTERNAL_SOURCE_FILE_COLUMN,
    INTERNAL_SOURCE_TYPE_COLUMN,
    MENTAL_SCHOOL_POPULATION_LABEL,
    POPULATION_COLUMN,
    RESEARCH_FEATURES,
    preprocess_yrbs_data,
)


BUCKET_NAME = "student-mental-health-lake-nhom1-2026"
GOLD_PREFIXES = {
    "survey_overview_summary": "gold/dashboard_tables/survey_overview_summary/",
    "survey_response_by_date": "gold/dashboard_tables/survey_response_by_date/",
    "survey_demographic_summary": "gold/dashboard_tables/survey_demographic_summary/",
    "survey_question_distribution": "gold/dashboard_tables/survey_question_distribution/",
    "survey_numeric_summary": "gold/dashboard_tables/survey_numeric_summary/",
    "survey_analytic_features": "gold/dashboard_tables/survey_analytic_features/",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dashboard Gold tables from fixed standardized survey CSVs.")
    parser.add_argument("--input-dir", default=str(PROJECT_ROOT / "build" / "survey_standardized_fixed"))
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "build" / "survey_gold_fixed"))
    parser.add_argument("--run-id", default="survey_standardized_fixed_" + datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"))
    parser.add_argument("--upload-gcs", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw = load_standardized(input_dir)
    processed = preprocess_yrbs_data(raw)
    analytic = build_analytic_features(processed.raw_analysis, processed.cleaned)
    tables = {
        "survey_analytic_features": analytic,
        "survey_overview_summary": build_overview_summary(analytic, args.run_id),
        "survey_response_by_date": build_response_by_date(analytic),
        "survey_demographic_summary": build_demographic_summary(analytic),
        "survey_question_distribution": build_question_distribution(analytic),
        "survey_numeric_summary": build_numeric_summary(analytic),
    }

    for name, table in tables.items():
        table = normalize_table_for_parquet(table.copy())
        table["run_id"] = args.run_id
        path = output_dir / name / f"run_id={args.run_id}"
        path.mkdir(parents=True, exist_ok=True)
        table.to_parquet(path / "part-00000.parquet", index=False)
        print(f"{name}: rows={len(table):,} cols={len(table.columns):,} -> {path}")

    audit(analytic)
    if args.upload_gcs:
        upload_tables(tables, args.run_id)


def load_standardized(input_dir: Path) -> pd.DataFrame:
    school = pd.read_csv(input_dir / "school_survey_standardized.csv", low_memory=False)
    school[INTERNAL_SOURCE_TYPE_COLUMN] = "school"
    school[INTERNAL_SOURCE_FILE_COLUMN] = "school_survey_standardized"

    university = pd.read_csv(input_dir / "university_survey_standardized.csv", low_memory=False)
    university[INTERNAL_SOURCE_TYPE_COLUMN] = "hms"
    university[INTERNAL_SOURCE_FILE_COLUMN] = university.get("source_dataset", "university_survey_standardized").fillna(
        "university_survey_standardized"
    )
    return pd.concat([school, university], ignore_index=True, sort=False)


def build_analytic_features(raw_analysis: pd.DataFrame, cleaned: pd.DataFrame) -> pd.DataFrame:
    analytic = raw_analysis.reset_index(drop=True).copy()
    cleaned = cleaned.reset_index(drop=True)
    analytic["Target"] = cleaned["Target"].astype(int)
    for feature in RESEARCH_FEATURES:
        analytic[feature] = pd.to_numeric(cleaned.get(feature), errors="coerce")
    analytic["source_group"] = analytic[POPULATION_COLUMN].map(
        {
            MENTAL_SCHOOL_POPULATION_LABEL: "school",
            HMS_POPULATION_LABEL: "university",
        }
    )
    analytic["source_dataset"] = analytic.get(DATA_SOURCE_COLUMN, analytic["source_group"])
    if "date" not in analytic.columns:
        analytic["date"] = pd.NaT
    return analytic


def build_overview_summary(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    total = len(df)
    return pd.DataFrame(
        [
            {
                "metric": "total_responses",
                "value": total,
                "school_responses": int((df["source_group"] == "school").sum()),
                "university_responses": int((df["source_group"] == "university").sum()),
                "at_risk_responses": int((df["Target"] == 1).sum()),
                "at_risk_rate": round(float(df["Target"].mean()), 6) if total else 0.0,
                "generated_at": datetime.utcnow().isoformat(),
                "source": "survey_standardized_fixed",
                "run_id": run_id,
            }
        ]
    )


def build_response_by_date(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    date = pd.to_datetime(work.get("date"), errors="coerce").dt.date
    work["date"] = date.astype("string").fillna("unknown")
    return (
        work.groupby("date", dropna=False)
        .agg(
            total_responses=("Target", "size"),
            school_responses=("source_group", lambda s: int((s == "school").sum())),
            university_responses=("source_group", lambda s: int((s == "university").sum())),
            at_risk_responses=("Target", "sum"),
            at_risk_rate=("Target", "mean"),
        )
        .reset_index()
    )


def build_demographic_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for dimension in ["source_group", "q1", "q2", "q3"]:
        if dimension not in df.columns:
            continue
        grouped = df.groupby(dimension, dropna=False)
        for value, group in grouped:
            rows.append(
                {
                    "dimension": dimension,
                    "value": str(value),
                    "count": len(group),
                    "at_risk_count": int(group["Target"].sum()),
                    "at_risk_rate": round(float(group["Target"].mean()), 6) if len(group) else 0.0,
                }
            )
    return pd.DataFrame(rows)


def build_question_distribution(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    q_columns = [column for column in df.columns if str(column).lower().startswith("q") and str(column)[1:].isdigit()]
    for column in q_columns:
        counts = df.groupby(["source_group", column], dropna=False).size().reset_index(name="count")
        total_by_source = counts.groupby("source_group")["count"].transform("sum")
        counts["percentage"] = counts["count"] / total_by_source
        counts["column_name"] = column
        counts = counts.rename(columns={column: "answer_value"})
        counts["answer_value"] = counts["answer_value"].astype("string")
        rows.append(counts[["column_name", "answer_value", "source_group", "count", "percentage"]])
    return pd.concat(rows, ignore_index=True, sort=False) if rows else pd.DataFrame()


def build_numeric_summary(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = [
        column
        for column in df.columns
        if pd.api.types.is_numeric_dtype(df[column]) and column not in {"Target"}
    ]
    rows = []
    for source_group, group in df.groupby("source_group", dropna=False):
        for column in numeric_cols:
            series = pd.to_numeric(group[column], errors="coerce").dropna()
            rows.append(
                {
                    "column_name": column,
                    "source_group": source_group,
                    "count": int(series.count()),
                    "avg": float(series.mean()) if not series.empty else None,
                    "min": float(series.min()) if not series.empty else None,
                    "max": float(series.max()) if not series.empty else None,
                    "stddev": float(series.std()) if series.count() > 1 else None,
                }
            )
    return pd.DataFrame(rows)


def audit(df: pd.DataFrame) -> None:
    print("AUDIT")
    print("rows", len(df), "columns", len(df.columns))
    print("source_group", df["source_group"].value_counts(dropna=False).to_dict())
    print("target", df["Target"].value_counts(dropna=False).to_dict())
    print("at_risk_rate", round(float(df["Target"].mean()) * 100, 2))
    for feature in RESEARCH_FEATURES:
        nonnull = int(pd.to_numeric(df[feature], errors="coerce").notna().sum()) if feature in df else 0
        print("feature", feature, "nonnull", nonnull)


def upload_tables(tables: dict[str, pd.DataFrame], run_id: str) -> None:
    import google.auth
    from google.cloud import storage

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if hasattr(credentials, "with_quota_project"):
        credentials = credentials.with_quota_project(None)
    client = storage.Client(project="student-mental-health-496205", credentials=credentials)
    bucket = client.bucket(BUCKET_NAME)
    for name, table in tables.items():
        payload = BytesIO()
        upload = normalize_table_for_parquet(table.copy())
        upload["run_id"] = run_id
        upload.to_parquet(payload, index=False)
        payload.seek(0)
        object_name = f"{GOLD_PREFIXES[name]}run_id={run_id}/part-00000.parquet"
        bucket.blob(object_name).upload_from_file(payload, content_type="application/octet-stream")
        print(f"uploaded gs://{BUCKET_NAME}/{object_name}")


def normalize_table_for_parquet(table: pd.DataFrame) -> pd.DataFrame:
    for column in table.columns:
        if table[column].dtype == object:
            table[column] = table[column].astype("string")
    return table


if __name__ == "__main__":
    main()
