"""Persistent on-disk cache helpers shared by the dashboard loaders."""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd


def dashboard_cache_dir() -> Path:
    return Path(os.getenv("DASHBOARD_CACHE_DIR", "data/.dashboard_cache"))


def atomic_pickle_dump(
    payload: Any,
    target: Path,
    *,
    keep_glob: str | None = None,
    keep_count: int = 3,
) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with NamedTemporaryFile(
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
        pd.to_pickle(payload, temporary_path)
        os.replace(temporary_path, target)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    if keep_glob:
        cached_files = sorted(
            target.parent.glob(keep_glob),
            key=lambda path: path.stat().st_mtime_ns,
            reverse=True,
        )
        for old_cache in cached_files[max(1, keep_count) :]:
            old_cache.unlink(missing_ok=True)
