from dataclasses import dataclass
from functools import lru_cache
import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(BACKEND_ROOT / ".env", override=True)


@dataclass(frozen=True)
class RAGSettings:
    document_dir: str = "data/raw/clinical_docs"
    gcs_bucket: str = "student-mental-health-lake-nhom1-2026"
    gcs_knowledge_base_prefix: str = "bronze/knowledge_base"
    gcs_bucket_name: str = "student-mental-health-lake-nhom1-2026"
    gcs_chatlog_prefix: str = "bronze/chat_logs"
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    openai_model: str = "gpt-5.4-mini"
    ollama_model: str = "llama3"
    qdrant_url: str = "http://34.60.71.130:6333"
    qdrant_collection: str = "student_mental_health"
    qdrant_vector_size: int = 384
    chunk_size: int = 500
    chunk_overlap: int = 100
    retrieval_k: int = 3
    retrieval_fetch_k: int = 6
    retrieval_score_threshold: float = 0.0
    max_context_chars: int = 1800
    max_context_doc_chars: int = 650
    max_history_messages: int = 4
    max_history_chars: int = 900
    enable_query_rewrite: bool = True
    query_rewrite_max_history_messages: int = 4
    query_rewrite_max_history_chars: int = 700
    emotion_model_path: str | None = None
    emotion_confidence_threshold: float = 0.6
    ollama_num_ctx: int = 2048
    ollama_num_gpu: int = 0
    ollama_num_predict: int = 256
    ollama_temperature: float = 0.2
    debug_rag: bool = True

    @property
    def qdrant_api_key(self) -> str | None:
        return os.getenv("QDRANT_API_KEY")

    @property
    def openai_api_key(self) -> str | None:
        return os.getenv("OPENAI_API_KEY")


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer.") from exc


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number.") from exc


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} must be a boolean.")


@lru_cache(maxsize=1)
def get_settings() -> RAGSettings:
    defaults = RAGSettings()
    return RAGSettings(
        document_dir=os.getenv("RAG_DOCUMENT_DIR", defaults.document_dir),
        gcs_bucket=os.getenv("GCS_BUCKET", defaults.gcs_bucket),
        gcs_knowledge_base_prefix=os.getenv("GCS_KNOWLEDGE_BASE_PREFIX", defaults.gcs_knowledge_base_prefix),
        gcs_bucket_name=os.getenv("GCS_BUCKET_NAME", os.getenv("GCS_BUCKET", defaults.gcs_bucket_name)),
        gcs_chatlog_prefix=os.getenv("GCS_CHATLOG_PREFIX", defaults.gcs_chatlog_prefix),
        embedding_model=os.getenv("RAG_EMBEDDING_MODEL", defaults.embedding_model),
        openai_model=os.getenv("OPENAI_MODEL", defaults.openai_model),
        ollama_model=os.getenv("OLLAMA_MODEL", defaults.ollama_model),
        qdrant_url=os.getenv("QDRANT_URL", defaults.qdrant_url),
        qdrant_collection=os.getenv("QDRANT_COLLECTION", defaults.qdrant_collection),
        qdrant_vector_size=_env_int("QDRANT_VECTOR_SIZE", defaults.qdrant_vector_size),
        chunk_size=_env_int("RAG_CHUNK_SIZE", defaults.chunk_size),
        chunk_overlap=_env_int("RAG_CHUNK_OVERLAP", defaults.chunk_overlap),
        retrieval_k=_env_int("RAG_RETRIEVAL_K", defaults.retrieval_k),
        retrieval_fetch_k=_env_int("RAG_RETRIEVAL_FETCH_K", defaults.retrieval_fetch_k),
        retrieval_score_threshold=_env_float(
            "RAG_RETRIEVAL_SCORE_THRESHOLD",
            defaults.retrieval_score_threshold,
        ),
        max_context_chars=_env_int("RAG_MAX_CONTEXT_CHARS", defaults.max_context_chars),
        max_context_doc_chars=_env_int("RAG_MAX_CONTEXT_DOC_CHARS", defaults.max_context_doc_chars),
        max_history_messages=_env_int("RAG_MAX_HISTORY_MESSAGES", defaults.max_history_messages),
        max_history_chars=_env_int("RAG_MAX_HISTORY_CHARS", defaults.max_history_chars),
        enable_query_rewrite=_env_bool("RAG_ENABLE_QUERY_REWRITE", defaults.enable_query_rewrite),
        query_rewrite_max_history_messages=_env_int(
            "RAG_QUERY_REWRITE_MAX_HISTORY_MESSAGES",
            defaults.query_rewrite_max_history_messages,
        ),
        query_rewrite_max_history_chars=_env_int(
            "RAG_QUERY_REWRITE_MAX_HISTORY_CHARS",
            defaults.query_rewrite_max_history_chars,
        ),
        emotion_model_path=os.getenv("RAG_EMOTION_MODEL_PATH", defaults.emotion_model_path),
        emotion_confidence_threshold=_env_float(
            "RAG_EMOTION_CONFIDENCE_THRESHOLD",
            defaults.emotion_confidence_threshold,
        ),
        ollama_num_ctx=_env_int("OLLAMA_NUM_CTX", defaults.ollama_num_ctx),
        ollama_num_gpu=_env_int("OLLAMA_NUM_GPU", defaults.ollama_num_gpu),
        ollama_num_predict=_env_int("OLLAMA_NUM_PREDICT", defaults.ollama_num_predict),
        ollama_temperature=_env_float("OLLAMA_TEMPERATURE", defaults.ollama_temperature),
        debug_rag=_env_bool("DEBUG_RAG", defaults.debug_rag),
    )
