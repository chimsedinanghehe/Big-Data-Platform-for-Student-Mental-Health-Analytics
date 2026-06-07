from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

from backend.db.connection import connect
from backend.db.surveys import derive_survey_state_for_profile


VALID_ROLES = {"student", "researcher"}
VALID_GENDERS = {"male", "female", "other"}
VALID_LEARNER_TYPES = {
    "elementary",
    "middle_school",
    "high_school",
    "college",
    "university",
    "graduate",
    "other",
}

HASH_ALGORITHM = "pbkdf2_sha256"
HASH_ITERATIONS = 390_000
SESSION_DAYS = 7


@dataclass(frozen=True)
class UserRecord:
    id: str
    email: str
    display_name: str
    role: str
    is_active: bool
    created_at: datetime
    updated_at: datetime
    profile: dict


@dataclass(frozen=True)
class AuthResult:
    access_token: str
    token_type: str
    expires_at: datetime
    user: UserRecord


def create_user(
    *,
    email: str,
    password: str,
    display_name: str,
    role: str,
    profile: dict | None = None,
) -> UserRecord:
    normalized_email = normalize_email(email)
    normalized_role = normalize_role(role)
    normalized_name = display_name.strip()
    if not normalized_name:
        raise ValueError("Display name is required.")
    if len(password) < 8:
        raise ValueError("Password must contain at least 8 characters.")
    normalized_profile = profile or {}
    if normalized_role == "student" and _profile_age(normalized_profile) is None:
        raise ValueError("Birth date is required for student survey routing.")
    survey_required, survey_type = derive_survey_state_for_profile(
        normalized_role,
        _profile_age(normalized_profile),
    )

    user_id = uuid4()
    password_hash = hash_password(password)

    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app_users (
                    id, email, password_hash, display_name, role, survey_required, survey_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id, email, display_name, role, is_active, created_at, updated_at
                """,
                (
                    user_id,
                    normalized_email,
                    password_hash,
                    normalized_name,
                    normalized_role,
                    survey_required,
                    survey_type,
                ),
            )
            row = cursor.fetchone()
            _upsert_profile(cursor, user_id, normalized_role, normalized_profile)
        connection.commit()

    if not row:
        raise RuntimeError("User insert did not return a row.")
    return _row_to_user_record(row, profile=normalized_profile)


def seed_user(
    *,
    email: str,
    password: str,
    display_name: str,
    role: str,
    profile: dict | None = None,
) -> UserRecord:
    normalized_email = normalize_email(email)
    normalized_role = normalize_role(role)
    password_hash = hash_password(password)
    normalized_profile = profile or {}
    survey_required, survey_type = derive_survey_state_for_profile(
        normalized_role,
        _profile_age(normalized_profile),
    )

    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app_users (
                    id, email, password_hash, display_name, role, survey_required, survey_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (email)
                DO UPDATE SET
                    password_hash = EXCLUDED.password_hash,
                    display_name = EXCLUDED.display_name,
                    role = EXCLUDED.role,
                    is_active = TRUE,
                    survey_required = CASE WHEN app_users.survey_completed THEN app_users.survey_required ELSE EXCLUDED.survey_required END,
                    survey_type = CASE WHEN app_users.survey_completed THEN app_users.survey_type ELSE EXCLUDED.survey_type END,
                    updated_at = NOW()
                RETURNING id, email, display_name, role, is_active, created_at, updated_at
                """,
                (
                    uuid4(),
                    normalized_email,
                    password_hash,
                    display_name.strip(),
                    normalized_role,
                    survey_required,
                    survey_type,
                ),
            )
            row = cursor.fetchone()
            if not row:
                raise RuntimeError("Seed user upsert did not return a row.")
            _upsert_profile(cursor, row["id"], normalized_role, normalized_profile)
        connection.commit()

    return get_user_by_email(normalized_email)  # type: ignore[return-value]


def authenticate_user(*, email: str, password: str) -> AuthResult | None:
    normalized_email = normalize_email(email)
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, email, password_hash, display_name, role, is_active, created_at, updated_at
                FROM app_users
                WHERE email = %s
                """,
                (normalized_email,),
            )
            row = cursor.fetchone()
            if not row or not row["is_active"] or not row["password_hash"]:
                return None
            if not verify_password(password, row["password_hash"]):
                return None

            raw_token = secrets.token_urlsafe(32)
            expires_at = datetime.now(timezone.utc) + timedelta(days=SESSION_DAYS)
            cursor.execute(
                """
                INSERT INTO app_sessions (id, user_id, token_hash, expires_at)
                VALUES (%s, %s, %s, %s)
                """,
                (uuid4(), row["id"], token_digest(raw_token), expires_at),
            )
        connection.commit()

    user = get_user_by_email(normalized_email)
    if user is None:
        raise RuntimeError("Authenticated user disappeared before response.")
    return AuthResult(access_token=raw_token, token_type="bearer", expires_at=expires_at, user=user)


def get_user_by_email(email: str) -> UserRecord | None:
    normalized_email = normalize_email(email)
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, email, display_name, role, is_active, created_at, updated_at
                FROM app_users
                WHERE email = %s
                """,
                (normalized_email,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            profile = _get_profile(cursor, row["id"], row["role"])
    return _row_to_user_record(row, profile=profile)


def get_user_by_token(access_token: str) -> UserRecord | None:
    digest = token_digest(access_token.strip())
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM app_sessions WHERE expires_at <= NOW()")
            cursor.execute(
                """
                SELECT u.id, u.email, u.display_name, u.role, u.is_active, u.created_at, u.updated_at
                FROM app_sessions s
                JOIN app_users u ON u.id = s.user_id
                WHERE s.token_hash = %s
                  AND s.expires_at > NOW()
                  AND u.is_active = TRUE
                """,
                (digest,),
            )
            row = cursor.fetchone()
            if not row:
                connection.commit()
                return None
            profile = _get_profile(cursor, row["id"], row["role"])
        connection.commit()
    return _row_to_user_record(row, profile=profile)


def update_user_profile(
    *,
    user_id: str,
    display_name: str,
    role: str,
    profile: dict,
) -> UserRecord:
    parsed_user_id = UUID(user_id)
    normalized_role = normalize_role(role)
    normalized_name = display_name.strip()
    if normalized_role == "student" and _profile_age(profile) is None:
        raise ValueError("Birth date is required for student survey routing.")
    survey_required, survey_type = derive_survey_state_for_profile(
        normalized_role,
        _profile_age(profile),
    )

    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE app_users
                SET display_name = %s,
                    role = %s,
                    survey_required = CASE WHEN survey_completed THEN survey_required ELSE %s END,
                    survey_type = CASE WHEN survey_completed THEN survey_type ELSE %s END,
                    survey_postponed = CASE
                        WHEN survey_completed THEN survey_postponed
                        WHEN survey_type IS DISTINCT FROM %s THEN FALSE
                        ELSE survey_postponed
                    END,
                    survey_postponed_at = CASE
                        WHEN survey_completed THEN survey_postponed_at
                        WHEN survey_type IS DISTINCT FROM %s THEN NULL
                        ELSE survey_postponed_at
                    END,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING id, email, display_name, role, is_active, created_at, updated_at
                """,
                (
                    normalized_name,
                    normalized_role,
                    survey_required,
                    survey_type,
                    survey_type,
                    survey_type,
                    parsed_user_id,
                ),
            )
            row = cursor.fetchone()
            if not row:
                raise RuntimeError("User not found.")
            _upsert_profile(cursor, parsed_user_id, normalized_role, profile)
        connection.commit()

    return get_user_by_email(row["email"])  # type: ignore[return-value]


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, HASH_ITERATIONS)
    return "$".join(
        [
            HASH_ALGORITHM,
            str(HASH_ITERATIONS),
            base64.b64encode(salt).decode("ascii"),
            base64.b64encode(digest).decode("ascii"),
        ]
    )


def verify_password(password: str, encoded_hash: str) -> bool:
    try:
        algorithm, iterations_raw, salt_raw, digest_raw = encoded_hash.split("$", maxsplit=3)
        if algorithm != HASH_ALGORITHM:
            return False
        iterations = int(iterations_raw)
        salt = base64.b64decode(salt_raw.encode("ascii"))
        expected = base64.b64decode(digest_raw.encode("ascii"))
    except Exception:
        return False

    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


def token_digest(access_token: str) -> str:
    return hashlib.sha256(access_token.encode("utf-8")).hexdigest()


def normalize_email(email: str) -> str:
    normalized = email.strip().lower()
    if "@" not in normalized or "." not in normalized.rsplit("@", maxsplit=1)[-1]:
        raise ValueError("A valid email address is required.")
    return normalized


def normalize_role(role: str) -> str:
    normalized = role.strip().lower()
    if normalized not in VALID_ROLES:
        raise ValueError("Role must be either student or researcher.")
    return normalized


def _upsert_profile(cursor: object, user_id: UUID, role: str, profile: dict) -> None:
    if role == "student":
        birth_date = _optional_date(profile.get("birth_date"))
        age = _profile_age(profile)
        gender = _optional_choice(profile.get("gender"), VALID_GENDERS, "gender")
        learner_type = _optional_choice(profile.get("learner_type"), VALID_LEARNER_TYPES, "learner_type")
        survey_type = _survey_type_for_age(age)
        cursor.execute("DELETE FROM researcher_profiles WHERE user_id = %s", (user_id,))
        cursor.execute(
            """
            INSERT INTO student_profiles (
                user_id,
                age,
                birth_date,
                gender,
                learner_type,
                survey_required,
                survey_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET
                age = EXCLUDED.age,
                birth_date = EXCLUDED.birth_date,
                gender = EXCLUDED.gender,
                learner_type = EXCLUDED.learner_type,
                survey_required = CASE
                    WHEN student_profiles.survey_completed THEN student_profiles.survey_required
                    ELSE EXCLUDED.survey_required
                END,
                survey_type = CASE
                    WHEN student_profiles.survey_completed THEN student_profiles.survey_type
                    ELSE EXCLUDED.survey_type
                END,
                survey_postponed = CASE
                    WHEN student_profiles.survey_completed THEN student_profiles.survey_postponed
                    WHEN student_profiles.survey_type IS DISTINCT FROM EXCLUDED.survey_type THEN FALSE
                    ELSE student_profiles.survey_postponed
                END,
                survey_postponed_at = CASE
                    WHEN student_profiles.survey_completed THEN student_profiles.survey_postponed_at
                    WHEN student_profiles.survey_type IS DISTINCT FROM EXCLUDED.survey_type THEN NULL
                    ELSE student_profiles.survey_postponed_at
                END,
                updated_at = NOW()
            """,
            (
                user_id,
                age,
                birth_date,
                gender,
                learner_type,
                age is not None,
                survey_type,
            ),
        )
    else:
        cursor.execute("DELETE FROM student_profiles WHERE user_id = %s", (user_id,))
        cursor.execute(
            """
            INSERT INTO researcher_profiles (user_id)
            VALUES (%s)
            ON CONFLICT (user_id)
            DO UPDATE SET updated_at = NOW()
            """,
            (user_id,),
        )


def _get_profile(cursor: object, user_id: UUID, role: str) -> dict:
    if role == "student":
        cursor.execute(
            """
            SELECT
                age,
                birth_date,
                gender,
                learner_type,
                survey_required,
                survey_completed,
                survey_type,
                survey_completed_at,
                survey_postponed,
                survey_postponed_at
            FROM student_profiles
            WHERE user_id = %s
            """,
            (user_id,),
        )
    else:
        return {}
    row = cursor.fetchone()
    if not row:
        return {}
    return dict(row)


def _row_to_user_record(row: dict, *, profile: dict) -> UserRecord:
    return UserRecord(
        id=str(row["id"]),
        email=str(row["email"]),
        display_name=str(row["display_name"]),
        role=str(row["role"]),
        is_active=bool(row["is_active"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        profile=profile,
    )


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _optional_int(value: object) -> int | None:
    if value in {None, ""}:
        return None
    return int(value)


def _optional_date(value: object) -> date | None:
    if value in {None, ""}:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value))


def _age_from_birth_date(birth_date: date | None, today: date | None = None) -> int | None:
    if birth_date is None:
        return None
    current = today or date.today()
    age = current.year - birth_date.year - ((current.month, current.day) < (birth_date.month, birth_date.day))
    if age < 5 or age > 100:
        raise ValueError("Birth date must produce an age between 5 and 100.")
    return age


def _profile_age(profile: dict) -> int | None:
    birth_date = _optional_date(profile.get("birth_date"))
    if birth_date is not None:
        return _age_from_birth_date(birth_date)
    return _optional_int(profile.get("age"))


def _optional_choice(value: object, allowed: set[str], field_name: str) -> str | None:
    normalized = _optional_text(value)
    if normalized is None:
        return None
    if normalized not in allowed:
        raise ValueError(f"{field_name} must be one of: {', '.join(sorted(allowed))}.")
    return normalized


def _survey_type_for_age(age: int | None) -> str | None:
    if age is None:
        return None
    return "school" if age <= 18 else "university"
