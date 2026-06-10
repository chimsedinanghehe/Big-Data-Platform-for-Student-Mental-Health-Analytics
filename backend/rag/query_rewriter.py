from backend.rag.generation.openai_client import generate_text, load_llm
from backend.rag.prompts import build_query_rewrite_prompt
from backend.rag.config import RAGSettings, get_settings


_CONTEXT_DEPENDENT_TERMS = {
    "cái này",
    "cái đó",
    "điều này",
    "điều đó",
    "việc này",
    "việc đó",
    "nó",
    "vậy",
    "thế",
    "tiếp",
    "nói thêm",
    "kỹ hơn",
    "rõ hơn",
    "có",
    "khác",
}


def rewrite_query(
    current_question: str,
    chat_history: list | None = None,
    settings: RAGSettings | None = None,
) -> str:
    settings = settings or get_settings()
    if not _should_rewrite(current_question, chat_history, settings):
        return current_question

    try:
        llm = load_llm(settings=settings)
        rewritten = generate_text(
            build_query_rewrite_prompt(
                current_question=current_question,
                chat_history=chat_history,
                max_history_messages=settings.query_rewrite_max_history_messages,
                max_history_chars=settings.query_rewrite_max_history_chars,
            ),
            settings=settings,
            client=llm,
        )
    except Exception:
        return current_question

    rewritten = str(rewritten).strip().strip('"').strip("'")
    if not rewritten:
        return current_question

    return rewritten.splitlines()[0].strip() or current_question


def _should_rewrite(
    current_question: str,
    chat_history: list | None,
    settings: RAGSettings,
) -> bool:
    if not settings.enable_query_rewrite or not chat_history:
        return False

    normalized = " ".join(current_question.lower().split())
    if len(normalized.split()) <= 8:
        return True

    return any(term in normalized for term in _CONTEXT_DEPENDENT_TERMS)
