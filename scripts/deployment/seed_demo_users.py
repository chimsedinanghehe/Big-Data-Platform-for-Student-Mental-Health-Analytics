from __future__ import annotations

import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.db.connection import initialize_schema_if_configured, require_database_url
from backend.db.users import seed_user


DEMO_STUDENT_PASSWORD = os.getenv("DEMO_STUDENT_PASSWORD")
DEMO_RESEARCHER_PASSWORD = os.getenv("DEMO_RESEARCHER_PASSWORD")

DEMO_USERS = [
    {
        "email": "student.demo@example.com",
        "password": DEMO_STUDENT_PASSWORD,
        "display_name": "Student Demo",
        "role": "user",
        "profile": {
            "birthday": "2006-01-01",
            "gender": "female",
            "learner_type": "university",
        },
    },
    {
        "email": "highschool.demo@example.com",
        "password": DEMO_STUDENT_PASSWORD,
        "display_name": "High School Demo",
        "role": "user",
        "profile": {
            "birthday": "2010-01-01",
            "gender": "other",
            "learner_type": "high_school",
        },
    },
    {
        "email": "researcher.demo@example.com",
        "password": DEMO_RESEARCHER_PASSWORD,
        "display_name": "Researcher Demo",
        "role": "researcher",
        "profile": {},
    },
]


def main() -> int:
    require_database_url()
    if not DEMO_STUDENT_PASSWORD or not DEMO_RESEARCHER_PASSWORD:
        raise RuntimeError(
            "Set DEMO_STUDENT_PASSWORD and DEMO_RESEARCHER_PASSWORD before seeding demo users."
        )
    initialize_schema_if_configured()
    print("Seeded demo accounts:")
    for item in DEMO_USERS:
        user = seed_user(**item)
        print(f"- {user.email} | role={user.role}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
