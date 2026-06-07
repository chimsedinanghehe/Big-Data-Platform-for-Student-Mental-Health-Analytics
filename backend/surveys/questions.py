from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SURVEY_FILES = {
    "school": PROJECT_ROOT / "school_survey_questions_answers.txt",
    "university": PROJECT_ROOT / "university_survey_questions_answers.txt",
}
VALID_SURVEY_TYPES = set(SURVEY_FILES)
PROFILE_MANAGED_QUESTION_IDS = {"age", "survey_date"}


@dataclass(frozen=True)
class SurveyQuestion:
    id: str
    prompt: str
    options: list[str]
    section: str
    map_to: str | None = None
    input_type: str = "select"
    required: bool = True

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "question": self.prompt,
            "prompt": self.prompt,
            "options": self.options,
            "section": self.section,
            "map_to": self.map_to,
            "map_columns": _parse_map_columns(self.map_to or ""),
            "input_type": self.input_type,
            "required": self.required,
        }


def survey_type_for_age(age: int | None) -> str | None:
    if age is None:
        return None
    return "school" if int(age) <= 18 else "university"


def audience_group_for_age(age: int | None) -> str:
    survey_type = survey_type_for_age(age)
    if survey_type == "school":
        return "school"
    if survey_type == "university":
        return "university"
    return "unknown"


def survey_questions(survey_type: str) -> list[dict[str, Any]]:
    return [question.as_dict() for question in _load_questions(survey_type)]


def expected_answer_ids(survey_type: str) -> list[str]:
    return [question.id for question in _load_questions(survey_type)]


def validate_and_normalize_answers(
    *,
    survey_type: str,
    answers: dict[str, Any],
) -> tuple[dict[str, str], dict[str, Any]]:
    questions = _load_questions(survey_type)
    by_id = {question.id: question for question in questions}
    cleaned: dict[str, str] = {}
    errors: dict[str, str] = {}

    for question in questions:
        value = answers.get(question.id)
        if value is None or str(value).strip() == "":
            if question.required:
                errors[question.id] = "Question is required."
            continue
        text_value = str(value).strip()
        if question.input_type == "date":
            try:
                date.fromisoformat(text_value)
            except ValueError:
                errors[question.id] = "Date must use YYYY-MM-DD."
                continue
        elif question.options and _norm(text_value) not in {_norm(option) for option in question.options}:
            errors[question.id] = "Answer is not one of the allowed options."
            continue
        cleaned[question.id] = text_value

    extra = sorted(set(answers) - set(by_id) - PROFILE_MANAGED_QUESTION_IDS)
    if extra:
        errors["_extra"] = f"Unknown question id(s): {', '.join(extra[:10])}."
    if errors:
        raise ValueError(errors)

    normalized = _normalize_school_answers(cleaned) if survey_type == "school" else _normalize_university_answers(cleaned)
    return cleaned, normalized


def _load_questions(survey_type: str) -> list[SurveyQuestion]:
    if survey_type not in VALID_SURVEY_TYPES:
        raise ValueError("survey_type must be school or university.")
    path = SURVEY_FILES[survey_type]
    if not path.exists():
        raise FileNotFoundError(f"Missing survey question file: {path}")
    return _parse_question_file(path)


def _parse_question_file(path: Path) -> list[SurveyQuestion]:
    lines = path.read_text(encoding="utf-8").splitlines()
    questions: list[SurveyQuestion] = []
    current_section = "Phần chung"
    current: dict[str, Any] | None = None
    reading_options = False

    def flush() -> None:
        nonlocal current
        if not current:
            return
        question_id = str(current["id"])
        if question_id in PROFILE_MANAGED_QUESTION_IDS:
            current = None
            return
        input_type = "date" if question_id.endswith("date") else "select"
        questions.append(
            SurveyQuestion(
                id=question_id,
                prompt=str(current.get("prompt") or question_id),
                options=list(current.get("options") or []),
                section=str(current.get("section") or current_section),
                map_to=current.get("map_to"),
                input_type=input_type,
            )
        )
        current = None

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if set(line) == {"="}:
            continue
        if line.startswith("PHAN ") or line.startswith("CUM "):
            flush()
            current_section = line
            continue

        question_match = re.match(r"^(\d+)\.\s+([A-Za-z0-9_]+)\s*$", line)
        if question_match:
            flush()
            current = {"id": question_match.group(2), "section": current_section, "options": []}
            reading_options = False
            continue
        if current is None:
            continue

        if line.startswith("Map "):
            current["map_to"] = line.split(":", 1)[-1].strip() if ":" in line else line
            reading_options = False
            continue
        if line.startswith("Cau hoi:"):
            current["prompt"] = line.split(":", 1)[1].strip()
            reading_options = False
            continue
        if line.startswith("Dap an"):
            reading_options = True
            continue
        if line.startswith("Ghi chu"):
            reading_options = False
            continue
        if reading_options and line.startswith("- "):
            current["options"].append(line[2:].strip())

    flush()
    return questions


def _normalize_school_answers(answers: dict[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "source_group": "school",
        "source_dataset": "app_school_survey",
    }
    mappings = {
        "gender": ("q2", _gender_q2_code),
        "school_grade": ("q3", _school_grade_code),
        "school_sad_hopeless_2weeks": ("q26", _yes_no_q_code),
        "school_poor_mental_health_days": ("q84", _frequency_1_5_code),
        "school_suicide_ideation": ("q27", _yes_no_q_code),
        "school_suicide_plan": ("q28", _yes_no_q_code),
        "school_suicide_attempt": ("q29", _attempt_yes_no_code),
        "school_unsafe_absence": ("q14", _days_0_6plus_code),
        "school_weapon_threat": ("q15", _times_0_12plus_code),
        "school_physical_fight": ("q16", _times_0_12plus_code),
        "school_bullied_school": ("q24", _yes_no_q_code),
        "school_bullied_online": ("q25", _yes_no_q_code),
        "school_forced_sexual_contact": ("q19", _yes_no_q_code),
        "school_dating_sexual_violence": ("q21", _dating_times_code),
        "school_dating_physical_violence": ("q22", _dating_times_code),
        "school_adult_verbal_abuse": ("q89", _frequency_1_5_code),
        "school_adult_physical_abuse": ("q90", _frequency_1_5_code),
        "school_family_violence_witness": ("q91", _frequency_1_5_code),
        "school_basic_needs_met": ("q99", _frequency_1_5_code),
        "school_parent_monitoring": ("q104", _frequency_1_5_code),
        "school_academic_performance": ("q87", _academic_grade_code),
        "school_school_belonging": ("q103", _agreement_1_5_code),
        "school_unfair_discipline": ("q105", _yes_no_q_code),
        "school_concentration_difficulty": ("q106", _yes_no_q_code),
        "school_smoking_current": ("q33", _days_0_30_code),
        "school_vaping_current": ("q36", _days_0_30_code),
        "school_alcohol_current": ("q42", _days_0_30_code),
        "school_sleep_hours": ("q85", _sleep_code),
        "school_physical_activity_days": ("q76", _days_0_7_code),
        "school_breakfast_frequency": ("q75", _days_0_7_code),
    }
    for question_id, (column, mapper) in mappings.items():
        if question_id in answers:
            out[column] = mapper(answers[question_id])
    out["grade"] = out.get("q3")
    out["gender"] = _q2_to_gender(out.get("q2"))
    return out


def _normalize_university_answers(answers: dict[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "source_group": "university",
        "source_dataset": "app_university_survey",
    }
    mappings = {
        "gender": ("gender", _university_gender_label),
        "university_year": ("grade", _university_year_code),
        "uni_depression_score": ("deprawsc", _severity_score),
        "uni_anxiety_score": ("anx_score", _severity_score),
        "uni_suicide_ideation": ("sui_idea", _yes_no_binary_code),
        "uni_suicide_plan": ("sui_plan", _yes_no_binary_code),
        "uni_suicide_attempt": ("sui_att", _yes_no_binary_code),
        "uni_financial_current": ("fincur", _leading_number),
        "uni_food_worry": ("food_worry", _leading_number),
        "uni_housing_worry": ("housing_worry", _leading_number),
        "uni_payment_worry": ("pay_worry", _leading_number),
        "uni_academic_impairment": ("aca_impa", _leading_number),
        "uni_academic_stress": ("aca_stress", _leading_number),
        "uni_competition_pressure": ("compet_sch", _leading_number),
        "uni_imposter_feeling": ("imposter_1", _leading_number),
        "uni_failed_course": ("failed", _yes_no_binary_code),
        "uni_time_management": ("time_manage", _leading_number),
        "uni_belonging": ("belong", _leading_number),
        "uni_discrimination": ("discrim", _yes_no_binary_code),
        "uni_campus_safety": ("safe_on", _leading_number),
        "uni_hostile_climate": ("hostcli", _leading_number),
        "uni_abuse_experience": ("abuse_life", _leading_number),
        "uni_stalking_experience": ("stalk_exp", _yes_no_binary_code),
        "uni_sexual_assault": ("assault_sex", _leading_number),
        "uni_partner_harm": ("partner_phys", _yes_no_binary_code),
        "uni_binge_drinking_frequency": ("binge_fr", _leading_number),
        "uni_substance_any": ("sub_any", _yes_no_binary_code),
        "uni_smoking_or_vaping": ("smok_vape", _smoking_vaping_code),
        "uni_weekday_sleep_hours": ("sleep_wknight", _leading_number),
        "uni_weekend_sleep_hours": ("sleep_wkend", _leading_number),
        "uni_exercise_frequency": ("exerc", _leading_number),
    }
    for question_id, (column, mapper) in mappings.items():
        if question_id in answers:
            out[column] = mapper(answers[question_id])
    out["yr_sch"] = out.get("grade")
    return out


def _norm(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", text.strip().lower())


def _parse_map_columns(raw_map: str) -> list[str]:
    if not raw_map:
        return []
    normalized = raw_map.replace("Map dashboard/YRBS:", "").replace("Map HMS:", "")
    columns = []
    for candidate in re.split(r"\s*(?:/|,|\bor\b)\s*", normalized, flags=re.IGNORECASE):
        column = candidate.strip().strip(".")
        if re.match(r"^[A-Za-z][A-Za-z0-9_]*$", column):
            columns.append(column.lower())
    return list(dict.fromkeys(columns))


def _identity(value: str) -> str:
    return value


def _leading_number(value: str) -> int | None:
    match = re.search(r"\d+", value)
    return int(match.group(0)) if match else None


def _yes_no_q_code(value: str) -> int | None:
    normalized = _norm(value)
    if normalized in {"yes", "y", "co"}:
        return 1
    if normalized in {"no", "n", "khong"}:
        return 2
    return None


def _yes_no_binary_code(value: str) -> int | None:
    normalized = _norm(value)
    if normalized.startswith("1:") or normalized in {"yes", "y", "co", "1"}:
        return 1
    if normalized.startswith("0:") or normalized in {"no", "n", "khong", "0"}:
        return 0
    return None


def _school_age_code(value: str) -> int | None:
    normalized = _norm(value)
    if "12" in normalized:
        return 1
    if normalized.startswith("13"):
        return 2
    if normalized.startswith("14"):
        return 3
    if normalized.startswith("15"):
        return 4
    if normalized.startswith("16"):
        return 5
    if normalized.startswith("17"):
        return 6
    if normalized.startswith("18"):
        return 7
    return None


def _school_q1_to_age(value: object) -> int | None:
    return {1: 12, 2: 13, 3: 14, 4: 15, 5: 16, 6: 17, 7: 18}.get(value)


def _gender_q2_code(value: str) -> int:
    normalized = _norm(value)
    if normalized in {"nu", "female"}:
        return 1
    if normalized in {"nam", "male"}:
        return 2
    return 3


def _q2_to_gender(value: object) -> str:
    return {1: "female", 2: "male"}.get(value, "other")


def _school_grade_code(value: str) -> int | None:
    number = _leading_number(value)
    if number is None:
        return None
    return {9: 1, 10: 2, 11: 3, 12: 4}.get(number)


def _frequency_1_5_code(value: str) -> int | None:
    order = ["never", "rarely", "sometimes", "most of the time", "always"]
    normalized = _norm(value)
    for index, label in enumerate(order, start=1):
        if normalized == label:
            return index
    return None


def _agreement_1_5_code(value: str) -> int | None:
    order = ["strongly agree", "agree", "not sure", "disagree", "strongly disagree"]
    normalized = _norm(value)
    for index, label in enumerate(order, start=1):
        if normalized == label:
            return index
    return None


def _days_0_6plus_code(value: str) -> int | None:
    return _ordered_option_code(value, ["0 days", "1 day", "2 or 3 days", "4 or 5 days", "6 or more days"])


def _days_0_30_code(value: str) -> int | None:
    return _ordered_option_code(
        value,
        ["0 days", "1 or 2 days", "3 to 5 days", "6 to 9 days", "10 to 19 days", "20 to 29 days", "all 30 days"],
    )


def _days_0_7_code(value: str) -> int | None:
    return _ordered_option_code(value, [f"{day} day" if day == 1 else f"{day} days" for day in range(8)])


def _times_0_12plus_code(value: str) -> int | None:
    return _ordered_option_code(
        value,
        ["0 times", "1 time", "2 or 3 times", "4 or 5 times", "6 or 7 times", "8 or 9 times", "10 or 11 times", "12 or more times"],
    )


def _dating_times_code(value: str) -> int | None:
    return _ordered_option_code(
        value,
        [
            "I did not date or go out with anyone during the past 12 months",
            "0 times",
            "1 time",
            "2 or 3 times",
            "4 or 5 times",
            "6 or more times",
        ],
    )


def _attempt_yes_no_code(value: str) -> int | None:
    code = _ordered_option_code(value, ["0 times", "1 time", "2 or 3 times", "4 or 5 times", "6 or more times"])
    if code is None:
        return None
    return 2 if code == 1 else 1


def _sleep_code(value: str) -> int | None:
    return _ordered_option_code(value, ["4 or less hours", "5 hours", "6 hours", "7 hours", "8 hours", "9 hours", "10 or more hours"])


def _academic_grade_code(value: str) -> int | None:
    return _ordered_option_code(
        value,
        ["Mostly A's", "Mostly B's", "Mostly C's", "Mostly D's", "Mostly F's", "None of these grades", "Not sure"],
    )


def _ordered_option_code(value: str, options: list[str]) -> int | None:
    normalized = _norm(value)
    for index, option in enumerate(options, start=1):
        if normalized == _norm(option):
            return index
    return None


def _severity_score(value: str) -> int | None:
    normalized = _norm(value)
    if normalized.startswith("0-4"):
        return 0
    if normalized.startswith("5-9"):
        return 5
    if normalized.startswith("10-14"):
        return 10
    if normalized.startswith("15-19") or normalized.startswith("15-21"):
        return 15
    if normalized.startswith("20-27"):
        return 20
    return _leading_number(value)


def _university_gender_label(value: str) -> str:
    normalized = _norm(value)
    if normalized in {"nam", "male"}:
        return "male"
    if normalized in {"nu", "female"}:
        return "female"
    return "other"


def _university_year_code(value: str) -> int | None:
    normalized = _norm(value)
    if "sau dai hoc" in normalized:
        return 6
    if "khac" in normalized:
        return 7
    number = _leading_number(value)
    if number is None:
        return None
    return min(number, 5)


def _smoking_vaping_code(value: str) -> int | None:
    normalized = _norm(value)
    if normalized == "no":
        return 0
    if normalized == "yes":
        return 1
    return _leading_number(value)
