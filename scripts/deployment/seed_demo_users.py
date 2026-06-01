from __future__ import annotations

import sys
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://student_app:student_app_password@127.0.0.1:5433/student_mental_health_app",
)

from backend.db.connection import initialize_schema_if_configured
from backend.db.users import seed_user


DEMO_USERS = [
    {
        "email": "student.demo@example.com",
        "password": "StudentDemo123!",
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
        "password": "StudentDemo123!",
        "display_name": "High School Demo",
        "role": "user",
        "profile": {
            "birthday": "2010-01-01",
            "gender": "other",
            "learner_type": "high_school",
        },
    },
]


def main() -> int:
    initialize_schema_if_configured()
    print("Seeded demo accounts:")
    for item in DEMO_USERS:
        user = seed_user(**item)
        print(f"- {user.email} | role={user.role} | password={item['password']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
