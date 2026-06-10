from backend.rag.generation.openai_client import generate_text, load_llm
from backend.rag.prompts import build_query_rewrite_prompt


def rewrite_query(current_question: str, chat_history: list | None = None) -> str:
    if not chat_history:
        return current_question

    try:
        llm = load_llm()
        rewritten = generate_text(
            build_query_rewrite_prompt(
                current_question=current_question,
                chat_history=chat_history,
            ),
            client=llm,
        )
    except Exception:
        return current_question

    rewritten = str(rewritten).strip().strip('"').strip("'")
    if not rewritten:
        return current_question

    return rewritten.splitlines()[0].strip() or current_question
