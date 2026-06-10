from __future__ import annotations

import re
from typing import Any


YES_VALUES = {"yes", "y", "co", "có"}
NO_VALUES = {"no", "n", "khong", "không"}


def map_answer_to_dashboard_value(survey_type: str, question: dict[str, Any], answer: Any) -> float | int | str | None:
    if answer is None:
        return None
    raw = str(answer).strip()
    if not raw:
        return None

    lowered = raw.lower()
    numeric_prefix = re.match(r"^\s*(\d+)(?:\s*-\s*\d+)?\s*:", raw)
    if numeric_prefix:
        return int(numeric_prefix.group(1))

    if lowered in YES_VALUES:
        return 1
    if lowered in NO_VALUES:
        return 2 if survey_type == "school" else 0

    frequency_level = re.search(r"frequency level\s+(\d+)", lowered)
    if frequency_level:
        return int(frequency_level.group(1))

    school_year = re.search(r"\bnam\s+(\d+)", lowered)
    if school_year:
        return int(school_year.group(1))
    if "sau dai hoc" in lowered or "graduate" in lowered:
        return 6

    options = list(question.get("options") or [])
    if raw in options:
        return options.index(raw) + 1

    if re.match(r"^[+-]?(?:\d+\.?\d*|\.\d+)$", raw):
        value = float(raw)
        return int(value) if value.is_integer() else value

    return raw


def derive_gender_columns(answer: Any) -> dict[str, int | str | None]:
    raw = "" if answer is None else str(answer).strip()
    lowered = raw.lower()
    is_male = "nam" in lowered and "khac" not in lowered
    is_female = "nu" in lowered or "nữ" in lowered
    is_nonbinary = "phi nhi" in lowered or "nonbin" in lowered or "genderqueer" in lowered
    is_trans = "chuyen gioi" in lowered or "trans" in lowered
    return {
        "gender": raw or None,
        "sex": raw or None,
        "sex_birth": 1 if is_male else 2 if is_female else None,
        "gender_male": 1 if is_male else 0,
        "gender_female": 1 if is_female else 0,
        "gender_nonbin": 1 if is_nonbinary else 0,
        "gender_queer": 1 if is_nonbinary else 0,
        "gender_trans": 1 if is_trans else 0,
        "gender_transm": 0,
        "gender_transf": 0,
    }


def dashboard_columns_for_answer(survey_type: str, question: dict[str, Any], answer: Any) -> dict[str, Any]:
    question_id = str(question.get("id", ""))
    columns = list(question.get("map_columns") or [])
    if question_id == "gender":
        return derive_gender_columns(answer)
    if question_id == "survey_date":
        return {"date": answer}
    if question_id == "age":
        return {"age": map_answer_to_dashboard_value(survey_type, question, answer)}
    if question_id == "school_grade":
        return {"grade": map_answer_to_dashboard_value(survey_type, question, answer)}
    if question_id == "university_year":
        return {"yr_sch": map_answer_to_dashboard_value(survey_type, question, answer), "grade": answer}

    mapped_value = map_answer_to_dashboard_value(survey_type, question, answer)
    return {column: mapped_value for column in columns}
