from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = Path(__file__).with_name("schema.sql")

load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(BACKEND_ROOT / ".env", override=True)


def get_database_url() -> str | None:
    value = os.getenv("DATABASE_URL")
    return value.strip() if value and value.strip() else None


def require_database_url() -> str:
    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")
    return database_url


def database_connect_timeout_seconds() -> int:
    value = os.getenv("DATABASE_CONNECT_TIMEOUT_SECONDS", "10")
    try:
        return max(1, int(value))
    except ValueError:
        return 10


@contextmanager
def connect() -> Iterator[object]:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError("Missing PostgreSQL dependency. Run: venv\\Scripts\\python.exe -m pip install -r requirements.txt") from exc

    with psycopg.connect(
        require_database_url(),
        row_factory=dict_row,
        connect_timeout=database_connect_timeout_seconds(),
    ) as connection:
        yield connection


def initialize_schema_if_configured() -> None:
    if not get_database_url():
        print("DATABASE_URL is not configured; user database is disabled.")
        return

    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(schema_sql)
        connection.commit()
