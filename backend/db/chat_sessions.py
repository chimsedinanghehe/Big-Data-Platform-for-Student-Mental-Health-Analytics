from __future__ import annotations

from uuid import UUID

from backend.chat_logs.gcs_writer import anonymize_session_id
from backend.db.connection import connect
from backend.db.users import VALID_GENDERS, VALID_LEARNER_TYPES
from backend.surveys.questions import survey_type_for_age


def upsert_chat_session_user_mapping(
    *,
    session_id: str,
    user_id: str,
    age: int | None,
    gender: str | None = None,
    learner_type: str | None = None,
) -> None:
    anonymous_session_id = anonymize_session_id(session_id)
    parsed_user_id = UUID(user_id)
    survey_type = survey_type_for_age(age)
    normalized_gender = _optional_choice(gender, VALID_GENDERS)
    normalized_learner_type = _optional_choice(learner_type, VALID_LEARNER_TYPES)
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO chat_session_user_map (
                    anonymous_session_id,
                    user_id,
                    survey_type,
                    age,
                    gender,
                    learner_type,
                    user_group
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (anonymous_session_id)
                DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    survey_type = EXCLUDED.survey_type,
                    age = EXCLUDED.age,
                    gender = EXCLUDED.gender,
                    learner_type = EXCLUDED.learner_type,
                    user_group = EXCLUDED.user_group,
                    updated_at = NOW()
                """,
                (
                    anonymous_session_id,
                    parsed_user_id,
                    survey_type,
                    age,
                    normalized_gender,
                    normalized_learner_type,
                    survey_type,
                ),
            )
        connection.commit()


def _optional_choice(value: str | None, allowed: set[str]) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized if normalized in allowed else None
