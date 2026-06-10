from backend.rag.config import RAGSettings, get_settings
from backend.rag.emotion import classify_emotion, emotion_signal_to_metadata, format_emotion_signal
from backend.rag.generation.openai_client import generate_response, load_llm
from backend.rag.indexing.embeddings import load_embedding_model
from backend.rag.indexing.qdrant_store import get_vector_store
from backend.rag.query_rewriter import rewrite_query
from backend.rag.retrieval.retriever import get_retriever
from backend.rag.safety import apply_safety_guardrails, detect_crisis_type


def answer_question(
    question: str,
    settings: RAGSettings | None = None,
    chat_history: list[dict[str, str]] | None = None,
    return_metadata: bool = False,
) -> str | dict:
    settings = settings or get_settings()

    print("Classifying emotional signal...")
    emotion_signal = classify_emotion(question, settings=settings)
    emotional_signal = format_emotion_signal(emotion_signal)
    emotion_metadata = emotion_signal_to_metadata(emotion_signal)
    crisis_type = detect_crisis_type(question)
    safety_metadata = {
        "crisis_flag": crisis_type is not None,
        "crisis_type": crisis_type,
    }

    print("Rewriting query...")
    standalone_query = rewrite_query(question, chat_history=chat_history)

    print("Loading embedding model...")
    embeddings = load_embedding_model(settings=settings)

    print("Connecting to Qdrant vector store...")
    vector_store = get_vector_store(embeddings, settings=settings)

    print("Creating retriever...")
    retriever = get_retriever(vector_store, settings=settings)

    print("Retrieving relevant documents...")
    docs = retriever.invoke(standalone_query)

    if not docs:
        answer = apply_safety_guardrails("No relevant documents found.", question)
        if return_metadata:
            return {
                "answer": answer,
                "sources": [],
                "standalone_query": standalone_query,
                "emotion": emotion_metadata,
                "safety": safety_metadata,
            }
        return answer

    context = format_context(docs, max_chars=settings.max_context_chars)

    print("Loading OpenAI response model...")
    llm = load_llm(settings=settings)

    print("Generating response...")
    answer = generate_response(
        llm=llm,
        context=context,
        question=question,
        settings=settings,
        standalone_query=standalone_query,
        chat_history=chat_history,
        emotional_signal=emotional_signal,
    )

    answer = apply_safety_guardrails(answer, question)
    sources = _source_labels(docs)

    if return_metadata:
        return {
            "answer": answer.strip(),
            "sources": sources,
            "standalone_query": standalone_query,
            "emotion": emotion_metadata,
            "safety": safety_metadata,
        }

    return add_source_attribution(answer, docs)


def ask_question(question: str, chat_history: list[dict[str, str]] | None = None) -> str:
    return answer_question(question, chat_history=chat_history)


def format_context(docs, max_chars: int | None = None) -> str:
    sections = []
    for index, doc in enumerate(docs, start=1):
        metadata = doc.metadata
        label = _source_label(metadata, index)
        sections.append(f"[{label}]\n{doc.page_content}")
    context = "\n\n".join(sections)
    if max_chars is None or len(context) <= max_chars:
        return context
    return context[:max_chars].rsplit(" ", maxsplit=1)[0].strip() + "..."


def add_source_attribution(answer: str, docs) -> str:
    sources = [f"- {source}" for source in _source_labels(docs)]

    if not sources:
        return answer

    return f"{answer.strip()}\n\nSources:\n" + "\n".join(sources)


def _source_labels(docs) -> list[str]:
    sources = []
    seen = set()
    for index, doc in enumerate(docs, start=1):
        label = _source_label(doc.metadata, index)
        if label in seen:
            continue
        seen.add(label)
        sources.append(label)
    return sources


def _source_label(metadata: dict, fallback_index: int) -> str:
    source_file = metadata.get("source_file") or "unknown source"
    page = metadata.get("page")
    chunk_id = metadata.get("chunk_id") or f"chunk-{fallback_index}"
    doc_type = metadata.get("doc_type") or "document"

    page_label = "unknown page" if page is None else f"page {int(page) + 1}"
    return f"{source_file}, {page_label}, {chunk_id}, {doc_type}"
