from pathlib import Path

from langchain_text_splitters import RecursiveCharacterTextSplitter

from backend.rag.config import RAGSettings, get_settings


def split_documents(documents, settings: RAGSettings | None = None):
    settings = settings or get_settings()
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=settings.chunk_size,
        chunk_overlap=settings.chunk_overlap,
        separators=["\n\n", "\n", ".", " "],
    )

    chunks = splitter.split_documents(documents)
    for index, chunk in enumerate(chunks):
        source = chunk.metadata.get("source", "")
        source_path = Path(source)
        chunk.metadata["source_file"] = source_path.name if source else "unknown"
        chunk.metadata["page"] = chunk.metadata.get("page")
        chunk.metadata["chunk_id"] = f"{source_path.stem or 'document'}-{chunk.metadata.get('page', 'na')}-{index}"
        chunk.metadata["doc_type"] = _infer_doc_type(source_path)

    print(f"Created {len(chunks)} chunks")
    return chunks


def _infer_doc_type(path: Path) -> str:
    if path.suffix.lower() == ".pdf":
        return "clinical_pdf"
    return "document"

