from functools import lru_cache

from langchain_huggingface import HuggingFaceEmbeddings

from backend.rag.config import RAGSettings, get_settings


def load_embedding_model(settings: RAGSettings | None = None):
    settings = settings or get_settings()
    return _load_embedding_model(settings.embedding_model)


@lru_cache(maxsize=4)
def _load_embedding_model(model_name: str):
    return HuggingFaceEmbeddings(model_name=model_name)
