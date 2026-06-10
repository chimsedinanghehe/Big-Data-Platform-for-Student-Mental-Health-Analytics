from langchain_huggingface import HuggingFaceEmbeddings

from backend.rag.config import RAGSettings, get_settings


def load_embedding_model(settings: RAGSettings | None = None):
    settings = settings or get_settings()
    return HuggingFaceEmbeddings(model_name=settings.embedding_model)

