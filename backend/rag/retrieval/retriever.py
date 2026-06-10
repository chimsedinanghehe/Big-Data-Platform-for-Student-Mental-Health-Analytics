from backend.rag.config import RAGSettings, get_settings


def get_retriever(vector_store, settings: RAGSettings | None = None):
    settings = settings or get_settings()
    return FastCompactRetriever(vector_store=vector_store, settings=settings)


class FastCompactRetriever:
    """Small retrieval adapter that keeps LangChain compatibility while cutting context noise."""

    def __init__(self, vector_store, settings: RAGSettings):
        self.vector_store = vector_store
        self.settings = settings

    def invoke(self, query: str):
        fetch_k = max(self.settings.retrieval_k, self.settings.retrieval_fetch_k)
        scored_docs = self._similarity_search(query=query, k=fetch_k)
        return _dedupe_and_trim_results(
            scored_docs,
            limit=self.settings.retrieval_k,
            score_threshold=self.settings.retrieval_score_threshold,
        )

    def _similarity_search(self, query: str, k: int):
        if hasattr(self.vector_store, "similarity_search_with_relevance_scores"):
            try:
                return list(self.vector_store.similarity_search_with_relevance_scores(query, k=k))
            except NotImplementedError:
                pass

        docs = self.vector_store.similarity_search(query, k=k)
        return [(doc, None) for doc in docs]


def _dedupe_and_trim_results(scored_docs, limit: int, score_threshold: float):
    docs = []
    seen = set()

    for doc, score in scored_docs:
        if score is not None and score < score_threshold:
            continue

        key = _document_key(doc)
        if key in seen:
            continue

        seen.add(key)
        if score is not None:
            doc.metadata["_retrieval_score"] = float(score)
        docs.append(doc)

        if len(docs) >= limit:
            break

    return docs


def _document_key(doc) -> tuple:
    metadata = doc.metadata or {}
    chunk_id = metadata.get("chunk_id") or metadata.get("chunk_index")
    if chunk_id is not None:
        return ("chunk", metadata.get("source_file"), metadata.get("page"), chunk_id)

    normalized_content = " ".join(str(doc.page_content).split())
    return ("content", normalized_content[:240])
