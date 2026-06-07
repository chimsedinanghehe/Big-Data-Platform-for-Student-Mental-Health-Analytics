from __future__ import annotations

import argparse
import sys
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.surveys.definitions import load_survey_questions


PROJECT_ID = "student-mental-health-496205"
COMMON_COLUMNS = {
    "age",
    "gender",
    "sex",
    "grade",
    "q1",
    "q2",
    "q3",
    "date",
    "startdate",
    "recordeddate",
    "created_at",
    "submitted_at",
    "survey_date",
    "source_file",
    "source_group",
    "source_dataset",
}

SCHOOL_REQUIRED_COLUMNS = {"q1", "q2", "q3", "age", "gender", "grade", "date"}
UNIVERSITY_REQUIRED_COLUMNS = {
    "date",
    "discrim_race",
    "discrim_culture",
    "discrim_gender",
    "discrim_sexual",
    "discrim_other",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create reduced school/university survey CSVs from legacy wide CSV files."
    )
    parser.add_argument("--school-input", required=True, help="Local path or gs:// URI for the legacy school CSV.")
    parser.add_argument(
        "--university-input",
        action="append",
        required=True,
        help="Local path or gs:// URI for a legacy university CSV. Repeat for multiple files.",
    )
    parser.add_argument("--output-dir", required=True, help="Local directory or gs:// prefix for standardized CSV outputs.")
    return parser.parse_args()


def allowed_columns(survey_type: str) -> set[str]:
    columns = set(COMMON_COLUMNS)
    for question in load_survey_questions(survey_type):
        columns.update(str(column).lower() for column in question.get("map_columns", []))
    if survey_type == "school":
        columns.update(SCHOOL_REQUIRED_COLUMNS)
    if survey_type == "university":
        columns.update(UNIVERSITY_REQUIRED_COLUMNS)
    return columns


def read_csv(uri: str) -> pd.DataFrame:
    if uri.startswith("gs://"):
        from google.cloud import storage

        parsed = urlparse(uri)
        client = create_storage_client()
        payload = client.bucket(parsed.netloc).blob(parsed.path.lstrip("/")).download_as_bytes()
        return pd.read_csv(BytesIO(payload), low_memory=False)
    return pd.read_csv(uri, low_memory=False)


def write_csv(frame: pd.DataFrame, output_dir: str, filename: str) -> str:
    if output_dir.startswith("gs://"):
        parsed = urlparse(output_dir.rstrip("/") + "/" + filename)
        payload = frame.to_csv(index=False).encode("utf-8")
        client = create_storage_client()
        client.bucket(parsed.netloc).blob(parsed.path.lstrip("/")).upload_from_string(payload, content_type="text/csv")
        return f"gs://{parsed.netloc}/{parsed.path.lstrip('/')}"

    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    output_path = path / filename
    frame.to_csv(output_path, index=False, encoding="utf-8")
    return str(output_path)


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = {column: str(column).strip().lower() for column in frame.columns}
    return frame.rename(columns=renamed)


def create_storage_client():
    import google.auth
    from google.cloud import storage

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if hasattr(credentials, "with_quota_project"):
        credentials = credentials.with_quota_project(None)
    return storage.Client(project=PROJECT_ID, credentials=credentials)


def reduce_frame(frame: pd.DataFrame, survey_type: str, source_name: str) -> pd.DataFrame:
    normalized = normalize_columns(frame)
    allowed = allowed_columns(survey_type)
    keep = [column for column in normalized.columns if column in allowed]
    reduced = normalized.loc[:, keep].copy()
    reduced = enrich_required_columns(reduced, survey_type)
    reduced["source_group"] = survey_type
    reduced["source_dataset"] = source_name
    return reduced


def enrich_required_columns(frame: pd.DataFrame, survey_type: str) -> pd.DataFrame:
    out = frame.copy()
    if survey_type == "school":
        if "q1" in out.columns and "age" not in out.columns:
            out["age"] = out["q1"].map(school_q1_to_age)
        if "q2" in out.columns and "gender" not in out.columns:
            out["gender"] = out["q2"].map(q2_to_gender)
        if "q3" in out.columns and "grade" not in out.columns:
            out["grade"] = out["q3"].map(school_q3_to_grade)
        for column in SCHOOL_REQUIRED_COLUMNS:
            if column not in out.columns:
                out[column] = pd.NA
        return out

    out = coalesce_date_columns(out)
    for column in UNIVERSITY_REQUIRED_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    return out


def coalesce_date_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if "date" in frame.columns:
        return frame
    candidates = [column for column in ["survey_date", "startdate", "recordeddate", "created_at", "submitted_at"] if column in frame.columns]
    if not candidates:
        frame["date"] = pd.NA
        return frame
    parsed = [pd.to_datetime(frame[column], errors="coerce").dt.date.astype("string") for column in candidates]
    date_values = parsed[0]
    for values in parsed[1:]:
        date_values = date_values.fillna(values)
    frame["date"] = date_values
    return frame


def school_q1_to_age(value: object) -> int | None:
    code = numeric_code(value)
    return {1: 12, 2: 13, 3: 14, 4: 15, 5: 16, 6: 17, 7: 18}.get(code)


def q2_to_gender(value: object) -> str | None:
    code = numeric_code(value)
    return {1: "female", 2: "male"}.get(code, "other" if code is not None else None)


def school_q3_to_grade(value: object) -> int | None:
    code = numeric_code(value)
    return {1: 9, 2: 10, 3: 11, 4: 12}.get(code)


def numeric_code(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def main() -> None:
    args = parse_args()
    school = reduce_frame(read_csv(args.school_input), "school", "school_survey_standardized")
    school_uri = write_csv(school, args.output_dir, "school_survey_standardized.csv")

    university_frames = [
        reduce_frame(read_csv(uri), "university", f"university_survey_{index}")
        for index, uri in enumerate(args.university_input, start=1)
    ]
    university = pd.concat(university_frames, ignore_index=True, sort=False)
    university_uri = write_csv(university, args.output_dir, "university_survey_standardized.csv")

    print(
        {
            "school_rows": int(len(school)),
            "school_columns": int(len(school.columns)),
            "school_output": school_uri,
            "university_rows": int(len(university)),
            "university_columns": int(len(university.columns)),
            "university_output": university_uri,
        }
    )


if __name__ == "__main__":
    main()
