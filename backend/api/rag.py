from uuid import uuid4

from fastapi import APIRouter, HTTPException, status
from qdrant_client.http.exceptions import ApiException, ResponseHandlingException, UnexpectedResponse

from backend.api.schemas import ErrorResponse, RAGAskRequest, RAGAskResponse
from backend.chat_logs.gcs_writer import write_chat_turn
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
def ask_rag(request: RAGAskRequest) -> RAGAskResponse:
    question = (request.question or "").strip()
    if not question:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "missing_question", "message": "Question is required."},
        )

    session_id = (request.session_id or str(uuid4())).strip()
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
            model="mock-model"
        )
    except Exception as exc:
        print(f"Kafka error: {exc}")

    return RAGAskResponse(
        answer=answer,
        session_id=session_id,
    )


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
