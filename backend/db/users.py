from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

from backend.db.connection import connect


VALID_ROLES = {"user"}
LEGACY_ROLES = {"student", "researcher"}
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

    user_id = uuid4()
    password_hash = hash_password(password)

    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app_users (id, email, password_hash, display_name, role)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, email, display_name, role, is_active, created_at, updated_at
                """,
                (user_id, normalized_email, password_hash, normalized_name, normalized_role),
            )
            row = cursor.fetchone()
            _upsert_profile(cursor, user_id, normalized_role, profile or {})
        connection.commit()

    if not row:
        raise RuntimeError("User insert did not return a row.")
    return _row_to_user_record(row, profile=profile or {})


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

    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app_users (id, email, password_hash, display_name, role)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (email)
                DO UPDATE SET
                    password_hash = EXCLUDED.password_hash,
                    display_name = EXCLUDED.display_name,
                    role = EXCLUDED.role,
                    is_active = TRUE,
                    updated_at = NOW()
                RETURNING id, email, display_name, role, is_active, created_at, updated_at
                """,
                (uuid4(), normalized_email, password_hash, display_name.strip(), normalized_role),
            )
            row = cursor.fetchone()
            if not row:
                raise RuntimeError("Seed user upsert did not return a row.")
            _upsert_profile(cursor, row["id"], normalized_role, profile or {})
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

    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE app_users
                SET display_name = %s,
                    role = %s,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING id, email, display_name, role, is_active, created_at, updated_at
                """,
                (normalized_name, normalized_role, parsed_user_id),
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
    if normalized in VALID_ROLES or normalized in LEGACY_ROLES:
        return "user"
    raise ValueError("Role must be user.")


def _upsert_profile(cursor: object, user_id: UUID, role: str, profile: dict) -> None:
    birthday = _optional_date(profile.get("birthday"))
    gender = _optional_choice(profile.get("gender"), VALID_GENDERS, "gender")
    learner_type = _optional_choice(profile.get("learner_type"), VALID_LEARNER_TYPES, "learner_type")
    cursor.execute(
        """
        INSERT INTO student_profiles (user_id, birthday, gender, learner_type)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id)
        DO UPDATE SET
            birthday = EXCLUDED.birthday,
            gender = EXCLUDED.gender,
            learner_type = EXCLUDED.learner_type,
            updated_at = NOW()
        """,
        (
            user_id,
            birthday,
            gender,
            learner_type,
        ),
    )


def _get_profile(cursor: object, user_id: UUID, role: str) -> dict:
    if role == "user":
        cursor.execute(
            """
            SELECT birthday, gender, learner_type
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


def _optional_date(value: object) -> date | None:
    if value in {None, ""}:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _optional_choice(value: object, allowed: set[str], field_name: str) -> str | None:
    normalized = _optional_text(value)
    if normalized is None:
        return None
    if normalized not in allowed:
        raise ValueError(f"{field_name} must be one of: {', '.join(sorted(allowed))}.")
    return normalized
