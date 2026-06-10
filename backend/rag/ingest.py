from backend.rag.config import RAGSettings, get_settings
from backend.rag.indexing.chunker import split_documents
from backend.rag.indexing.embeddings import load_embedding_model
from backend.rag.indexing.qdrant_store import get_indexed_sources, get_vector_store
from backend.rag.loaders.pdf_loader import load_gcs_pdf_documents


def build_index(settings: RAGSettings | None = None) -> int:
    settings = settings or get_settings()

    print("Checking already indexed documents in Qdrant...")
    indexed_sources = get_indexed_sources(settings=settings)
    print(f"Found {len(indexed_sources)} already indexed source file(s)")

    print("Loading documents from Google Cloud Storage...")
    documents = load_gcs_pdf_documents(
        bucket_name=settings.gcs_bucket,
        prefix=settings.gcs_knowledge_base_prefix,
        skip_sources=indexed_sources,
    )

    if not documents:
        print("Indexing skipped: no new documents to add")
        return 0

    print("Splitting documents...")
    chunks = split_documents(documents, settings=settings)

    print("Loading embedding model...")
    embeddings = load_embedding_model(settings=settings)

    print("Connecting to Qdrant vector store...")
    vector_store = get_vector_store(embeddings, settings=settings)

    print("Adding chunks to Qdrant...")
    vector_store.add_documents(chunks)

    print(f"Indexing completed successfully: {len(chunks)} chunks added")
    return len(chunks)


def build_pipeline() -> int:
    return build_index()
