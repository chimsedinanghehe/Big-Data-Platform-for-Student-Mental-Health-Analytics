from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, status
from pydantic import BaseModel, Field

from backend.db.surveys import SurveyAlreadyCompletedError, get_survey_status, postpone_survey, submit_survey_response
from backend.db.users import get_user_by_token
from backend.surveys.questions import survey_questions


router = APIRouter(prefix="/api/survey", tags=["survey"])


class SurveyStatusResponse(BaseModel):
    user_id: str
    age: int | None
    survey_type: str | None
    survey_completed: bool
    survey_required: bool
    survey_postponed: bool
    survey_completed_at: datetime | None
    show_survey_prompt: bool
    show_survey_tab: bool


class SurveyQuestionsResponse(BaseModel):
    survey_type: str
    questions: list[dict[str, Any]]


class SurveySubmitRequest(BaseModel):
    survey_type: str | None = Field(default=None)
    answers: dict[str, Any] = Field(default_factory=dict)


class SurveySubmitResponse(BaseModel):
    status: SurveyStatusResponse
    message: str


@router.get("/status", response_model=SurveyStatusResponse)
def read_survey_status(authorization: str | None = Header(default=None)) -> SurveyStatusResponse:
    user = _require_user(authorization)
    try:
        return _status_response(get_survey_status(user_id=user.id, role=user.role))
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "survey_status_unavailable", "message": str(exc)},
        ) from exc


@router.get("/questions", response_model=SurveyQuestionsResponse)
def read_survey_questions(
    authorization: str | None = Header(default=None),
    survey_type: str | None = Query(default=None),
) -> SurveyQuestionsResponse:
    user = _require_user(authorization)
    try:
        status_record = get_survey_status(user_id=user.id, role=user.role)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "survey_status_unavailable", "message": str(exc)},
        ) from exc

    effective_type = survey_type or status_record.survey_type
    if effective_type != status_record.survey_type:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "survey_type_mismatch", "message": "Survey type does not match the user's registered age."},
        )
    if not effective_type or not status_record.survey_required:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "survey_not_required", "message": "This user does not need a survey."},
        )
    if status_record.survey_completed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "survey_already_completed", "message": "Survey has already been completed."},
        )
    return SurveyQuestionsResponse(survey_type=effective_type, questions=survey_questions(effective_type))


@router.post("/postpone", response_model=SurveyStatusResponse)
def postpone_current_survey(authorization: str | None = Header(default=None)) -> SurveyStatusResponse:
    user = _require_user(authorization)
    try:
        return _status_response(postpone_survey(user_id=user.id, role=user.role))
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "survey_postpone_failed", "message": str(exc)},
        ) from exc


@router.post("/submit", response_model=SurveySubmitResponse)
def submit_current_survey(
    request: SurveySubmitRequest,
    authorization: str | None = Header(default=None),
) -> SurveySubmitResponse:
    user = _require_user(authorization)
    try:
        status_record = get_survey_status(user_id=user.id, role=user.role)
        survey_type = status_record.survey_type
        if not survey_type:
            raise ValueError("Survey is not required for this user.")
        if request.survey_type and request.survey_type != survey_type:
            raise ValueError("Survey type does not match the user's registered age.")
        updated_status = submit_survey_response(
            user_id=user.id,
            role=user.role,
            answers=request.answers,
        )
    except SurveyAlreadyCompletedError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "survey_already_completed", "message": str(exc)},
        ) from exc
    except ValueError as exc:
        detail = exc.args[0]
        if isinstance(detail, str) and "already" in detail.lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "survey_already_completed", "message": detail},
            ) from exc
        message = "Survey payload is invalid." if isinstance(detail, dict) else str(exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "invalid_survey", "message": message, "fields": detail if isinstance(detail, dict) else {}},
        ) from exc
    except Exception as exc:
        text = str(exc)
        if "unique" in text.lower() or "already" in text.lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "survey_already_completed", "message": "Survey has already been completed."},
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "survey_submit_failed", "message": text},
        ) from exc

    return SurveySubmitResponse(status=_status_response(updated_status), message="Survey completed.")


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


def _status_response(record) -> SurveyStatusResponse:
    return SurveyStatusResponse(
        user_id=record.user_id,
        age=record.age,
        survey_type=record.survey_type,
        survey_completed=record.survey_completed,
        survey_required=record.survey_required,
        survey_postponed=record.survey_postponed,
        survey_completed_at=record.survey_completed_at,
        show_survey_prompt=record.show_survey_prompt,
        show_survey_tab=record.show_survey_tab,
    )
