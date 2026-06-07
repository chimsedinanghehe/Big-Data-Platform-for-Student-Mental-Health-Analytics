from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, status
from qdrant_client.http.exceptions import ApiException, ResponseHandlingException, UnexpectedResponse

from backend.api.schemas import ErrorResponse, RAGAskRequest, RAGAskResponse
from backend.db.chat_sessions import upsert_chat_session_user_mapping
from backend.db.surveys import get_chat_profile_context, get_survey_status
from backend.db.users import get_user_by_token
from backend.rag.config import get_settings
from backend.rag.service import answer_question


router = APIRouter(prefix="/api/rag", tags=["rag"])


@router.post(
    "/ask",
    response_model=RAGAskResponse,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
def ask_rag(request: RAGAskRequest, authorization: str | None = Header(default=None)) -> RAGAskResponse:
    question = (request.question or "").strip()
    if not question:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "missing_question", "message": "Question is required."},
        )

    session_id = (request.session_id or str(uuid4())).strip()
    current_user = _optional_user(authorization)
    survey_status = None
    profile_context = {}
    if current_user is not None:
        user_profile = current_user.profile or {}
        try:
            upsert_chat_session_user_mapping(
                session_id=session_id,
                user_id=current_user.id,
                age=user_profile.get("age"),
                gender=user_profile.get("gender"),
                learner_type=user_profile.get("learner_type"),
            )
            survey_status = get_survey_status(user_id=current_user.id, role=current_user.role)
            profile_context = get_chat_profile_context(user_id=current_user.id)
        except Exception as exc:
            print(f"Chat user mapping error: {exc}")
    chat_history = [
        {"role": message.role, "content": message.content}
        for message in request.chat_history
        if message.content.strip()
    ]
    settings = get_settings()

    try:
        result = answer_question(
            question=question,
            settings=settings,
            chat_history=chat_history,
            return_metadata=True,
        )
    except RuntimeError as exc:
        if "QDRANT" in str(exc):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"error": "qdrant_configuration_error", "message": "Qdrant vector store is not configured."},
            ) from exc
        if _looks_like_generation_error(exc):
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail={"error": "openai_unavailable", "message": str(exc)},
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "rag_runtime_error", "message": "RAG service failed while processing the request."},
        ) from exc
    except (ApiException, ResponseHandlingException, UnexpectedResponse) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "qdrant_unavailable", "message": "Qdrant vector store is unavailable."},
        ) from exc
    except Exception as exc:
        if _looks_like_generation_error(exc):
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail={"error": "openai_unavailable", "message": "OpenAI generation service is unavailable."},
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "internal_error", "message": "Unexpected RAG service error."},
        ) from exc

    if isinstance(result, dict):
        answer = str(result.get("answer", "")).strip()
        sources = list(result.get("sources", []))
        standalone_query = result.get("standalone_query")
        emotion = result.get("emotion")
        safety = result.get("safety")
    else:
        answer, sources = _split_answer_sources(str(result))
        standalone_query = question
        emotion = None
        safety = None

    if answer.startswith("No relevant documents found."):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "empty_retrieval", "message": "No relevant documents were found for the question."},
        )

    # THAY THẾ ĐOẠN GỌI GCS CŨ BẰNG KAFKA
    try:
        from backend.chat_logs.kafka_publisher import send_chat_turn_to_kafka
        send_chat_turn_to_kafka(
            session_id=session_id,
            question=question,
            answer=answer,
            is_document_rag=bool(sources),
            model="mock-model",
            user_id=current_user.id if current_user else None,
            user_age=profile_context.get("age") if profile_context else ((current_user.profile or {}).get("age") if current_user else None),
            user_gender=profile_context.get("gender") if profile_context else ((current_user.profile or {}).get("gender") if current_user else None),
            learner_type=profile_context.get("learner_type") if profile_context else ((current_user.profile or {}).get("learner_type") if current_user else None),
            grade=profile_context.get("grade") if profile_context else None,
            class_level=profile_context.get("class_level") if profile_context else None,
            user_group=profile_context.get("survey_type") if profile_context else (survey_status.survey_type if survey_status else None),
            survey_type=profile_context.get("survey_type") if profile_context else (survey_status.survey_type if survey_status else None),
            survey_completed=bool(profile_context.get("survey_completed")) if profile_context else (survey_status.survey_completed if survey_status else None),
        )
    except Exception as exc:
        print(f"Kafka error: {exc}")

    return RAGAskResponse(
        answer=answer,
        session_id=session_id,
    )


def _optional_user(authorization: str | None):
    if not authorization or not authorization.lower().startswith("bearer "):
        return None
    token = authorization.split(" ", maxsplit=1)[1].strip()
    if not token:
        return None
    return get_user_by_token(token)


def _split_answer_sources(raw_answer: str) -> tuple[str, list[str]]:
    marker = "\n\nSources:\n"
    if marker not in raw_answer:
        return raw_answer.strip(), []

    answer, source_block = raw_answer.split(marker, maxsplit=1)
    sources = []
    trailing_answer_lines = []

    for line in source_block.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            sources.append(stripped.removeprefix("- ").strip())
        elif stripped:
            trailing_answer_lines.append(stripped)

    answer_parts = [answer.strip()]
    if trailing_answer_lines:
        answer_parts.append("\n".join(trailing_answer_lines))

    return "\n\n".join(answer_parts).strip(), sources


def _looks_like_generation_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "openai" in text
        or "api key" in text
        or "connection refused" in text
        or "failed to establish a new connection" in text
    )
