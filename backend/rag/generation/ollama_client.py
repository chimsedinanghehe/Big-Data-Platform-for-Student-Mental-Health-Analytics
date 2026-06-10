from langchain_community.llms import Ollama

from backend.rag.config import RAGSettings, get_settings
from backend.rag.prompts import build_grounded_prompt


def load_llm(settings: RAGSettings | None = None):
    settings = settings or get_settings()
    return Ollama(
        model=settings.ollama_model,
        num_ctx=settings.ollama_num_ctx,
        num_gpu=settings.ollama_num_gpu,
        num_predict=settings.ollama_num_predict,
        temperature=settings.ollama_temperature,
    )


def generate_response(
    llm,
    context: str,
    question: str,
    settings: RAGSettings | None = None,
    standalone_query: str | None = None,
    chat_history: list[dict[str, str]] | None = None,
):
    settings = settings or get_settings()
    return llm.invoke(
        build_grounded_prompt(
            context=context,
            question=question,
            standalone_query=standalone_query,
            chat_history=chat_history,
            max_history_messages=settings.max_history_messages,
            max_history_chars=settings.max_history_chars,
        )
    )
