from backend.rag.config import RAGSettings, get_settings


def get_retriever(vector_store, settings: RAGSettings | None = None):
    settings = settings or get_settings()
    return vector_store.as_retriever(
        search_type="similarity",
        search_kwargs={"k": settings.retrieval_k},
    )

