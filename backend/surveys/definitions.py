from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SURVEY_DEFINITION_FILES = {
    "school": PROJECT_ROOT / "school_survey_questions_answers.txt",
    "university": PROJECT_ROOT / "university_survey_questions_answers.txt",
}

SURVEY_TITLES = {
    "school": "Khảo sát sức khỏe học sinh",
    "university": "Khảo sát sức khỏe sinh viên",
}

QUESTION_HEADER_RE = re.compile(r"^\s*(\d+)\.\s+([A-Za-z0-9_]+)\s*$")
MAP_SPLIT_RE = re.compile(r"\s*(?:/|,|\bor\b)\s*", re.IGNORECASE)
PROFILE_MANAGED_QUESTION_IDS = {"age", "survey_date"}


def survey_type_for_age(age: int | None) -> str | None:
    if age is None:
        return None
    return "school" if age <= 18 else "university"


def normalize_survey_type(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in SURVEY_DEFINITION_FILES:
        raise ValueError("survey_type must be either school or university.")
    return normalized


@lru_cache(maxsize=2)
def load_survey_questions(survey_type: str) -> list[dict[str, Any]]:
    normalized_type = normalize_survey_type(survey_type)
    path = SURVEY_DEFINITION_FILES[normalized_type]
    if not path.exists():
        raise FileNotFoundError(f"Survey definition file is missing: {path}")

    questions: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    section = "Thông tin chung"

    def flush_current() -> None:
        nonlocal current
        if current is not None:
            if current.get("id") not in PROFILE_MANAGED_QUESTION_IDS:
                current["input_type"] = infer_input_type(current)
                questions.append(current)
            current = None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        match = QUESTION_HEADER_RE.match(line)
        if match:
            flush_current()
            current = {
                "order": int(match.group(1)),
                "id": match.group(2),
                "section": section,
                "question": "",
                "options": [],
                "map_columns": [],
                "required": True,
            }
            continue

        if current is None:
            if line.startswith("PHAN ") or line.startswith("CUM "):
                section = normalize_section_title(line)
            continue

        if line.startswith("CUM "):
            section = normalize_section_title(line)
            current["section"] = section
            continue
        if line.startswith("Cau hoi:"):
            current["question"] = line.split(":", maxsplit=1)[1].strip()
            continue
        if line.startswith("Map "):
            raw_map = line.split(":", maxsplit=1)[1].strip() if ":" in line else ""
            current["map_columns"] = parse_map_columns(raw_map)
            continue
        if line.startswith("- "):
            option = line.removeprefix("- ").strip()
            if option:
                current["options"].append(option)

    flush_current()

    if not questions:
        raise ValueError(f"No survey questions were parsed from {path}")
    return questions


def get_survey_definition(survey_type: str) -> dict[str, Any]:
    normalized_type = normalize_survey_type(survey_type)
    return {
        "survey_type": normalized_type,
        "title": SURVEY_TITLES[normalized_type],
        "questions": load_survey_questions(normalized_type),
    }


def expected_answer_ids(survey_type: str) -> list[str]:
    return [question["id"] for question in load_survey_questions(survey_type)]


def validate_answers(survey_type: str, answers: dict[str, Any]) -> dict[str, Any]:
    questions = load_survey_questions(survey_type)
    by_id = {question["id"]: question for question in questions}
    normalized_answers: dict[str, Any] = {}
    missing: list[str] = []
    invalid: list[str] = []

    for question in questions:
        question_id = question["id"]
        value = answers.get(question_id)
        normalized = "" if value is None else str(value).strip()
        if question.get("required") and not normalized:
            missing.append(question_id)
            continue

        if normalized and question.get("input_type") == "date":
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", normalized):
                invalid.append(question_id)
                continue
        elif normalized and question.get("options") and normalized not in set(question["options"]):
            invalid.append(question_id)
            continue

        normalized_answers[question_id] = normalized

    extra = sorted(set(answers) - set(by_id))
    if missing or invalid:
        details = []
        if missing:
            details.append(f"missing answers: {', '.join(missing)}")
        if invalid:
            details.append(f"invalid answers: {', '.join(invalid)}")
        raise ValueError("; ".join(details))

    for question_id in extra:
        value = answers.get(question_id)
        if value not in (None, ""):
            normalized_answers[question_id] = value

    return normalized_answers


def parse_map_columns(raw_map: str) -> list[str]:
    if not raw_map:
        return []
    normalized = raw_map.replace("Map dashboard/YRBS:", "").replace("Map HMS:", "")
    columns = []
    for candidate in MAP_SPLIT_RE.split(normalized):
        column = candidate.strip().strip(".")
        if not column:
            continue
        if " " in column and not re.match(r"^[A-Za-z0-9_]+$", column):
            continue
        if re.match(r"^[A-Za-z][A-Za-z0-9_]*$", column):
            columns.append(column.lower())
    return list(dict.fromkeys(columns))


def infer_input_type(question: dict[str, Any]) -> str:
    question_id = str(question.get("id", "")).lower()
    if question_id.endswith("date") or question_id == "survey_date":
        return "date"
    if question_id == "age":
        return "select"
    return "select"


def normalize_section_title(line: str) -> str:
    section = line.strip("= ").strip()
    if " - " in section:
        section = section.split(" - ", maxsplit=1)[1].strip()
    return section.title() if section.isupper() else section
