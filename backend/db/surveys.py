from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from backend.db.connection import connect
from backend.surveys.questions import expected_answer_ids, survey_type_for_age, validate_and_normalize_answers
from backend.surveys.kafka_publisher import publish_survey_completed_event


class SurveyAlreadyCompletedError(RuntimeError):
    pass


def derive_survey_state_for_profile(role: str, age: int | None) -> tuple[bool, str | None]:
    if role != "student" or age is None:
        return False, None
    return True, survey_type_for_age(age)


@dataclass(frozen=True)
class SurveyStatus:
    user_id: str
    age: int | None
    survey_type: str | None
    survey_completed: bool
    survey_required: bool
    survey_completed_at: datetime | None
    survey_postponed: bool
    show_survey_prompt: bool
    show_survey_tab: bool


def get_survey_status(*, user_id: str, role: str) -> SurveyStatus:
    if role != "student":
        return SurveyStatus(
            user_id=user_id,
            age=None,
            survey_type=None,
            survey_completed=True,
            survey_required=False,
            survey_completed_at=None,
            survey_postponed=False,
            show_survey_prompt=False,
            show_survey_tab=False,
        )

    parsed_user_id = UUID(user_id)
    with connect() as connection:
        with connection.cursor() as cursor:
            row = _profile_for_update(cursor, parsed_user_id, lock=False)
            if not row:
                return _empty_student_status(user_id)

            response_exists = _survey_response_exists(cursor, parsed_user_id)
            status = _status_from_profile(user_id, row, response_exists)
            if row["survey_type"] != status.survey_type and status.survey_type is not None:
                cursor.execute(
                    """
                    UPDATE student_profiles
                    SET survey_type = %s,
                        survey_required = TRUE,
                        updated_at = NOW()
                    WHERE user_id = %s
                    """,
                    (status.survey_type, parsed_user_id),
                )
        connection.commit()
    return status


def postpone_survey(*, user_id: str, role: str) -> SurveyStatus:
    if role != "student":
        raise ValueError("Survey is only required for student accounts.")

    parsed_user_id = UUID(user_id)
    with connect() as connection:
        with connection.cursor() as cursor:
            row = _profile_for_update(cursor, parsed_user_id, lock=True)
            if not row or row["age"] is None:
                raise ValueError("Student age is required before postponing survey.")

            response_exists = _survey_response_exists(cursor, parsed_user_id)
            if row["survey_completed"] or response_exists:
                return _status_from_profile(user_id, row, True)

            survey_type = survey_type_for_age(row["age"])
            cursor.execute(
                """
                UPDATE student_profiles
                SET survey_required = TRUE,
                    survey_type = %s,
                    survey_postponed = TRUE,
                    survey_postponed_at = NOW(),
                    updated_at = NOW()
                WHERE user_id = %s
                RETURNING
                    age,
                    survey_required,
                    survey_completed,
                    survey_type,
                    survey_completed_at,
                    survey_postponed
                """,
                (survey_type, parsed_user_id),
            )
            updated = cursor.fetchone()
            cursor.execute(
                """
                UPDATE app_users
                SET survey_required = TRUE,
                    survey_type = %s,
                    survey_postponed = TRUE,
                    survey_postponed_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (survey_type, parsed_user_id),
            )
        connection.commit()
    return _status_from_profile(user_id, updated, False)


def submit_survey_response(*, user_id: str, role: str, answers: dict) -> SurveyStatus:
    if role != "student":
        raise ValueError("Survey is only required for student accounts.")

    parsed_user_id = UUID(user_id)
    with connect() as connection:
        with connection.cursor() as cursor:
            row = _profile_for_update(cursor, parsed_user_id, lock=True)
            if not row or row["age"] is None:
                raise ValueError("Student age is required before submitting survey.")

            existing_response = _survey_response_exists(cursor, parsed_user_id)
            if row["survey_completed"] or existing_response:
                raise SurveyAlreadyCompletedError("This user has already completed the survey.")

            survey_type = survey_type_for_age(row["age"])
            if survey_type is None:
                raise ValueError("Unable to determine survey type from age.")

            cleaned_answers, _normalized_answers = validate_and_normalize_answers(
                survey_type=survey_type,
                answers=answers,
            )
            required_count = len(expected_answer_ids(survey_type))
            response_id = uuid4()
            cursor.execute(
                """
                INSERT INTO survey_responses (
                    id,
                    user_id,
                    survey_type,
                    answers,
                    question_count
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    response_id,
                    parsed_user_id,
                    survey_type,
                    Jsonb(cleaned_answers),
                    required_count,
                ),
            )

            cursor.execute(
                """
                UPDATE student_profiles
                SET survey_required = TRUE,
                    survey_completed = TRUE,
                    survey_type = %s,
                    survey_completed_at = NOW(),
                    survey_postponed = FALSE,
                    survey_postponed_at = NULL,
                    updated_at = NOW()
                WHERE user_id = %s
                RETURNING
                    age,
                    survey_required,
                    survey_completed,
                    survey_type,
                    survey_completed_at,
                    survey_postponed
                """,
                (survey_type, parsed_user_id),
            )
            updated = cursor.fetchone()
            cursor.execute(
                """
                UPDATE app_users
                SET survey_required = TRUE,
                    survey_completed = TRUE,
                    survey_type = %s,
                    survey_completed_at = NOW(),
                    survey_postponed = FALSE,
                    survey_postponed_at = NULL,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (survey_type, parsed_user_id),
            )
        connection.commit()

    publish_survey_completed_event(
        survey_response_id=str(response_id),
        user_id=str(parsed_user_id),
        survey_type=survey_type,
    )
    return _status_from_profile(user_id, updated, True)


def get_chat_profile_context(*, user_id: str) -> dict[str, object]:
    """Return profile/survey metadata that is safe to attach to chat logs."""
    parsed_user_id = UUID(user_id)
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    sp.age,
                    sp.gender,
                    sp.learner_type,
                    sp.survey_type AS profile_survey_type,
                    sp.survey_completed,
                    sr.survey_type AS response_survey_type,
                    sr.answers,
                    sr.created_at AS survey_submitted_at
                FROM student_profiles sp
                LEFT JOIN survey_responses sr ON sr.user_id = sp.user_id
                WHERE sp.user_id = %s
                """,
                (parsed_user_id,),
            )
            row = cursor.fetchone()

    if not row:
        return {}

    survey_type = row["response_survey_type"] or row["profile_survey_type"] or survey_type_for_age(row["age"])
    answers = row["answers"] or {}
    normalized_answers: dict[str, object] = {}
    if survey_type in {"school", "university"} and answers:
        try:
            _cleaned, normalized_answers = validate_and_normalize_answers(
                survey_type=survey_type,
                answers=dict(answers),
            )
        except Exception:
            normalized_answers = {}

    grade = normalized_answers.get("grade") or normalized_answers.get("yr_sch")
    normalized_age = normalized_answers.get("age")
    normalized_gender = normalized_answers.get("gender")
    return {
        "age": normalized_age if normalized_age is not None else row["age"],
        "gender": normalized_gender or row["gender"],
        "learner_type": row["learner_type"],
        "survey_type": survey_type,
        "survey_completed": bool(row["survey_completed"] or row["response_survey_type"]),
        "survey_submitted_at": row["survey_submitted_at"],
        "grade": grade,
        "class_level": grade or row["learner_type"],
    }


def _empty_student_status(user_id: str) -> SurveyStatus:
    return SurveyStatus(
        user_id=user_id,
        age=None,
        survey_type=None,
        survey_completed=False,
        survey_required=False,
        survey_completed_at=None,
        survey_postponed=False,
        show_survey_prompt=False,
        show_survey_tab=False,
    )


def _profile_for_update(cursor: object, user_id: UUID, *, lock: bool) -> dict | None:
    lock_clause = " FOR UPDATE" if lock else ""
    cursor.execute(
        f"""
        SELECT
            age,
            gender,
            learner_type,
            survey_required,
            survey_completed,
            survey_type,
            survey_completed_at,
            survey_postponed
        FROM student_profiles
        WHERE user_id = %s
        {lock_clause}
        """,
        (user_id,),
    )
    return cursor.fetchone()


def _survey_response_exists(cursor: object, user_id: UUID) -> bool:
    cursor.execute("SELECT 1 FROM survey_responses WHERE user_id = %s LIMIT 1", (user_id,))
    return cursor.fetchone() is not None


def _status_from_profile(user_id: str, row: dict, response_exists: bool) -> SurveyStatus:
    age = row["age"]
    survey_type = row["survey_type"] or survey_type_for_age(age)
    survey_required = bool(row["survey_required"]) and age is not None and survey_type is not None
    survey_completed = bool(row["survey_completed"]) or response_exists
    survey_postponed = bool(row["survey_postponed"]) and not survey_completed
    return SurveyStatus(
        user_id=user_id,
        age=age,
        survey_type=survey_type,
        survey_completed=survey_completed,
        survey_required=survey_required,
        survey_completed_at=row["survey_completed_at"],
        survey_postponed=survey_postponed,
        show_survey_prompt=survey_required and not survey_completed and not survey_postponed,
        show_survey_tab=survey_required and not survey_completed and survey_postponed,
    )
