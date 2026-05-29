"""Test Qdrant connection for the Mental Health Analytics Platform."""

from qdrant_client import QdrantClient

from backend.rag.config import get_settings


def test_qdrant_connection() -> bool:
    """Test the Qdrant connection using the configured settings."""
    settings = get_settings()

    print("Testing Qdrant connection...")
    print(f"Qdrant URL: {settings.qdrant_url}")
    print(f"Collection: {settings.qdrant_collection}")

    try:
        client = QdrantClient(url=settings.qdrant_url, api_key=settings.qdrant_api_key)
        service_info = client.info()
        print(f"Connected to Qdrant {service_info.version}")

        exists = client.collection_exists(settings.qdrant_collection)
        if exists:
            info = client.get_collection(settings.qdrant_collection)
            print(f"Collection exists: {settings.qdrant_collection}")
            print(f"Points count: {info.points_count}")
        else:
            print(f"Collection does not exist yet: {settings.qdrant_collection}")

        return True
    except Exception as exc:
        print(f"Failed to connect to Qdrant: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if test_qdrant_connection() else 1)
