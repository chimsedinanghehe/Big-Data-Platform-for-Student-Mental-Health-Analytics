from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Header, HTTPException, status
from pydantic import BaseModel, Field, validator

from backend.db.users import (
    VALID_GENDERS,
    VALID_LEARNER_TYPES,
    VALID_ROLES,
    authenticate_user,
    create_user,
    get_user_by_token,
    update_user_profile,
)


auth_router = APIRouter(prefix="/api/auth", tags=["auth"])
users_router = APIRouter(prefix="/api/users", tags=["users"])


class StudentProfilePayload(BaseModel):
    age: int | None = Field(default=None, ge=5, le=100)
    birth_date: date | None = None
    gender: str | None = None
    learner_type: str | None = None

    @validator("gender")
    @classmethod
    def validate_gender(cls, value: str | None) -> str | None:
        return _validate_optional_choice(value, VALID_GENDERS, "gender")

    @validator("learner_type")
    @classmethod
    def validate_learner_type(cls, value: str | None) -> str | None:
        return _validate_optional_choice(value, VALID_LEARNER_TYPES, "learner_type")


class ResearcherProfilePayload(BaseModel):
    pass


class RegisterRequest(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    password: str = Field(min_length=8, max_length=128)
    display_name: str = Field(min_length=1, max_length=120)
    role: str = "student"
    student_profile: StudentProfilePayload | None = None
    researcher_profile: ResearcherProfilePayload | None = None

    @validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        return _validate_email(value)

    @validator("role")
    @classmethod
    def validate_role(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in VALID_ROLES:
            raise ValueError("Role must be either student or researcher.")
        return normalized


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    password: str = Field(min_length=1, max_length=128)

    @validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        return _validate_email(value)


class ProfileUpdateRequest(BaseModel):
    display_name: str = Field(min_length=1, max_length=120)
    role: str
    student_profile: StudentProfilePayload | None = None
    researcher_profile: ResearcherProfilePayload | None = None

    @validator("role")
    @classmethod
    def validate_role(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in VALID_ROLES:
            raise ValueError("Role must be either student or researcher.")
        return normalized


class UserResponse(BaseModel):
    id: str
    email: str
    display_name: str
    role: str
    is_active: bool
    created_at: datetime
    updated_at: datetime
    profile: dict[str, Any]


class AuthResponse(BaseModel):
    access_token: str
    token_type: str
    expires_at: datetime
    user: UserResponse


@auth_router.post("/register", response_model=AuthResponse)
def register(request: RegisterRequest) -> AuthResponse:
    profile = _profile_for_role(request.role, request.student_profile, request.researcher_profile)
    try:
        create_user(
            email=request.email,
            password=request.password,
            display_name=request.display_name,
            role=request.role,
            profile=profile,
        )
        auth_result = authenticate_user(email=request.email, password=request.password)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "invalid_registration", "message": str(exc)},
        ) from exc
    except Exception as exc:
        if "duplicate key" in str(exc).lower() or "unique" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "email_exists", "message": "This email is already registered."},
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "database_unavailable", "message": str(exc)},
        ) from exc

    if auth_result is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "login_after_register_failed", "message": "Registration succeeded but login failed."},
        )
    return _auth_response(auth_result)


@auth_router.post("/login", response_model=AuthResponse)
def login(request: LoginRequest) -> AuthResponse:
    try:
        auth_result = authenticate_user(email=request.email, password=request.password)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "database_unavailable", "message": str(exc)},
        ) from exc

    if auth_result is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "invalid_credentials", "message": "Email or password is incorrect."},
        )
    return _auth_response(auth_result)


@auth_router.get("/me", response_model=UserResponse)
def read_current_user(authorization: str | None = Header(default=None)) -> UserResponse:
    return _user_response(_require_user(authorization))


@users_router.put("/me", response_model=UserResponse)
def save_current_user(
    request: ProfileUpdateRequest,
    authorization: str | None = Header(default=None),
) -> UserResponse:
    current_user = _require_user(authorization)
    profile = _profile_for_role(request.role, request.student_profile, request.researcher_profile)
    try:
        updated_user = update_user_profile(
            user_id=current_user.id,
            display_name=request.display_name,
            role=request.role,
            profile=profile,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "invalid_profile", "message": str(exc)},
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "database_unavailable", "message": str(exc)},
        ) from exc
    return _user_response(updated_user)


def _require_user(authorization: str | None):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "missing_token", "message": "Login is required."},
        )
    token = authorization.split(" ", maxsplit=1)[1].strip()
    user = get_user_by_token(token)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "invalid_token", "message": "Session is invalid or expired."},
        )
    return user


def _profile_for_role(
    role: str,
    student_profile: StudentProfilePayload | None,
    researcher_profile: ResearcherProfilePayload | None,
) -> dict[str, Any]:
    if role == "student":
        return student_profile.dict() if student_profile else {}
    return {}


def _auth_response(auth_result) -> AuthResponse:
    return AuthResponse(
        access_token=auth_result.access_token,
        token_type=auth_result.token_type,
        expires_at=auth_result.expires_at,
        user=_user_response(auth_result.user),
    )


def _user_response(user) -> UserResponse:
    return UserResponse(
        id=user.id,
        email=user.email,
        display_name=user.display_name,
        role=user.role,
        is_active=user.is_active,
        created_at=user.created_at,
        updated_at=user.updated_at,
        profile=user.profile,
    )


def _validate_email(value: str) -> str:
    normalized = value.strip().lower()
    if "@" not in normalized or "." not in normalized.rsplit("@", maxsplit=1)[-1]:
        raise ValueError("A valid email address is required.")
    return normalized


def _validate_optional_choice(value: str | None, allowed: set[str], field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    if normalized not in allowed:
        raise ValueError(f"{field_name} must be one of: {', '.join(sorted(allowed))}.")
    return normalized
