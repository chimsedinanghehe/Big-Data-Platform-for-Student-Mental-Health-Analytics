from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.http.models import FieldCondition, Filter, MatchAny
from sentence_transformers import SentenceTransformer


DEFAULT_QUERIES = [
    "How can students manage anxiety and panic?",
    "What should someone do if they feel suicidal?",
    "How can CBT help with low mood?",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke-test semantic search against Qdrant.")
    parser.add_argument("--collection", default=os.getenv("QDRANT_COLLECTION", "student_mental_health_v1"))
    parser.add_argument("--qdrant-url", default=os.getenv("QDRANT_URL"))
    parser.add_argument("--embedding-model", default=os.getenv("RAG_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"))
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--new-silver-only", action="store_true", help="Filter to known Bronze PDF files rebuilt in version=v2.")
    return parser.parse_args()


def main() -> int:
    load_env_files()
    args = parse_args()
    if not args.qdrant_url:
        raise RuntimeError("Missing --qdrant-url or QDRANT_URL")

    client = QdrantClient(url=args.qdrant_url, api_key=os.getenv("QDRANT_API_KEY"))
    model = SentenceTransformer(args.embedding_model)
    query_filter = rebuilt_source_filter() if args.new_silver_only else None

    for query in DEFAULT_QUERIES:
        vector = model.encode(query, normalize_embeddings=True).tolist()
        results = query_points(client, args.collection, vector, args.limit, query_filter)
        print(f"\nQUERY: {query}")
        for rank, point in enumerate(results, start=1):
            payload = point.payload or {}
            preview = str(payload.get("chunk_text", "")).replace("\n", " ")[:260]
            print(
                f"rank={rank} score={point.score:.4f} "
                f"source_file={payload.get('source_file')} page={payload.get('page')} "
                f"chunk_index={payload.get('chunk_index')} has_chunk_text={bool(payload.get('chunk_text'))}"
            )
            print(f"payload_keys={sorted(payload.keys())}")
            print(f"chunk_preview={preview}")

    return 0


def load_env_files() -> None:
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")
    load_dotenv(project_root / "backend" / ".env", override=True)


def rebuilt_source_filter() -> Filter:
    return Filter(
        must=[
            FieldCondition(
                key="source_file",
                match=MatchAny(
                    any=[
                        "student_mental_health_clinical_doc_sample.pdf",
                        "wellbeing-team-cbt-workshop-booklet-2016.pdf",
                    ]
                ),
            )
        ]
    )


def query_points(
    client: QdrantClient,
    collection_name: str,
    vector: list[float],
    limit: int,
    query_filter: Filter | None,
) -> list[Any]:
    if hasattr(client, "query_points"):
        response = client.query_points(
            collection_name=collection_name,
            query=vector,
            query_filter=query_filter,
            limit=limit,
            with_payload=True,
            with_vectors=False,
        )
        return list(response.points)

    return list(
        client.search(
            collection_name=collection_name,
            query_vector=vector,
            query_filter=query_filter,
            limit=limit,
            with_payload=True,
            with_vectors=False,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
