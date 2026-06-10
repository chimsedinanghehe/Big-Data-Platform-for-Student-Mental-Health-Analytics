from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.surveys.snapshot_worker import export_pending_survey_responses


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Single-writer worker for bronze/app_survey_snapshot/survey_all.parquet.")
    parser.add_argument("--once", action="store_true", help="Process one pending batch and exit.")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--poll-seconds", type=float, default=15.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    while True:
        result = export_pending_survey_responses(limit=args.limit)
        print(json.dumps(result, sort_keys=True, default=str))
        if args.once:
            return
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
