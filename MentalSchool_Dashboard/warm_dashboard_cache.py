"""Warm persistent Survey and Chat Gold caches after the nightly Spark refresh."""

from __future__ import annotations

import json
import logging

logging.getLogger("streamlit").setLevel(logging.ERROR)

from app import load_gold_survey_prepared_data, survey_gold_tables_manifest
from dashboard_cache import dashboard_cache_dir
from dashboard_gcs_loader import load_chat_gold_tables, update_chat_gold_current_manifest
from scripts.update_survey_gold_current_manifest import main as update_survey_manifest


def main() -> int:
    update_survey_manifest()

    survey_manifest, survey_error = survey_gold_tables_manifest()
    if not survey_manifest:
        raise RuntimeError(f"Survey Gold manifest is unavailable: {survey_error}")
    survey_scopes = {}
    for scope in ("overview", "school", "university"):
        _survey_data, survey_shape, survey_status = load_gold_survey_prepared_data(survey_manifest, scope)
        survey_scopes[scope] = {
            "shape": list(survey_shape),
            "status": survey_status,
        }

    update_chat_gold_current_manifest()
    chat_tables = load_chat_gold_tables()
    if chat_tables.get("chat_hourly_metrics") is None or chat_tables["chat_hourly_metrics"].empty:
        raise RuntimeError("Chat Gold cache warm failed: chat_hourly_metrics is empty.")

    result = {
        "status": "warmed",
        "cache_dir": str(dashboard_cache_dir()),
        "survey_scopes": survey_scopes,
        "chat_rows": {name: int(len(frame)) for name, frame in sorted(chat_tables.items())},
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
