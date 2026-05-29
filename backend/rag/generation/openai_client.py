from typing import Any

from backend.rag.config import RAGSettings, get_settings
from backend.rag.prompts import build_grounded_prompt


def load_llm(settings: RAGSettings | None = None) -> Any:
    settings = settings or get_settings()
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is required for OpenAI generation.")

    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError("The openai package is required. Install it with: pip install openai") from exc

    return OpenAI(api_key=settings.openai_api_key)


def generate_text(prompt: str, settings: RAGSettings | None = None, client: Any | None = None) -> str:
    settings = settings or get_settings()
    client = client or load_llm(settings=settings)

    response = client.responses.create(
        model=settings.openai_model,
        input=prompt,
    )

    return _extract_response_text(response)


def generate_response(
    llm: Any,
    context: str,
    question: str,
    settings: RAGSettings | None = None,
    standalone_query: str | None = None,
    chat_history: list[dict[str, str]] | None = None,
    emotional_signal: str | None = None,
) -> str:
    settings = settings or get_settings()
    prompt = build_grounded_prompt(
        context=context,
        question=question,
        standalone_query=standalone_query,
        chat_history=chat_history,
        emotional_signal=emotional_signal,
        max_history_messages=settings.max_history_messages,
        max_history_chars=settings.max_history_chars,
    )
    return generate_text(prompt, settings=settings, client=llm)


def _extract_response_text(response) -> str:
    output_text = getattr(response, "output_text", None)
    if output_text:
        return str(output_text).strip()

    parts: list[str] = []
    for output_item in getattr(response, "output", []) or []:
        for content_item in getattr(output_item, "content", []) or []:
            text = getattr(content_item, "text", None)
            if text:
                parts.append(str(text))

    text = "\n".join(parts).strip()
    if not text:
        raise RuntimeError("OpenAI response did not include output text.")
    return text
