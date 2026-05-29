from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams

from backend.rag.config import RAGSettings, get_settings


def get_vector_store(embeddings, settings: RAGSettings | None = None) -> QdrantVectorStore:
    settings = settings or get_settings()
    client = QdrantClient(url=settings.qdrant_url, api_key=settings.qdrant_api_key)
    _ensure_collection(client, settings)

    return QdrantVectorStore(
        client=client,
        collection_name=settings.qdrant_collection,
        embedding=embeddings,
    )


def get_qdrant_client(settings: RAGSettings | None = None) -> QdrantClient:
    settings = settings or get_settings()
    client = QdrantClient(url=settings.qdrant_url, api_key=settings.qdrant_api_key)
    _ensure_collection(client, settings)
    return client


def get_indexed_sources(settings: RAGSettings | None = None) -> set[str]:
    settings = settings or get_settings()
    client = get_qdrant_client(settings)
    indexed_sources: set[str] = set()
    offset = None

    while True:
        points, offset = client.scroll(
            collection_name=settings.qdrant_collection,
            limit=256,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )

        for point in points:
            payload = point.payload or {}
            source = payload.get("source")
            metadata = payload.get("metadata") or {}
            if not source and isinstance(metadata, dict):
                source = metadata.get("source")
            if source:
                indexed_sources.add(str(source))

        if offset is None:
            break

    return indexed_sources


def _ensure_collection(client: QdrantClient, settings: RAGSettings) -> None:
    if client.collection_exists(settings.qdrant_collection):
        return

    client.create_collection(
        collection_name=settings.qdrant_collection,
        vectors_config=VectorParams(
            size=settings.qdrant_vector_size,
            distance=Distance.COSINE,
        ),
    )
