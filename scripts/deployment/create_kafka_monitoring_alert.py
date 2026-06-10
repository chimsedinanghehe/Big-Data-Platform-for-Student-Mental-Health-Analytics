from __future__ import annotations

import json
import os
import sys

from google.auth import default
from google.auth.transport.requests import AuthorizedSession


PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "student-mental-health-496205")
KAFKA_INSTANCE_ID = os.getenv("KAFKA_INSTANCE_ID", "8647727623426856537")
DISPLAY_NAME = "Kafka VM uptime missing - student-chat-streaming-m"


def main() -> int:
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(credentials)
    base_url = f"https://monitoring.googleapis.com/v3/projects/{PROJECT_ID}"

    response = session.get(f"{base_url}/alertPolicies", timeout=30)
    response.raise_for_status()
    policies = response.json().get("alertPolicies", [])
    existing = next(
        (policy for policy in policies if policy.get("displayName") == DISPLAY_NAME),
        None,
    )
    if existing:
        print(json.dumps({"status": "exists", "name": existing["name"]}))
        return 0

    policy = {
        "displayName": DISPLAY_NAME,
        "documentation": {
            "content": (
                "Kafka VM student-chat-streaming-m stopped reporting uptime for at "
                "least 5 minutes. Check the VM, Kafka broker, consumer and SSH tunnel."
            ),
            "mimeType": "text/markdown",
        },
        "combiner": "OR",
        "enabled": True,
        "conditions": [
            {
                "displayName": "Kafka VM stops reporting uptime for 5 minutes",
                "conditionAbsent": {
                    "filter": (
                        'resource.type = "gce_instance" '
                        f'AND resource.label.instance_id = "{KAFKA_INSTANCE_ID}" '
                        'AND metric.type = "compute.googleapis.com/instance/uptime"'
                    ),
                    "aggregations": [
                        {
                            "alignmentPeriod": "60s",
                            "perSeriesAligner": "ALIGN_RATE",
                        }
                    ],
                    "duration": "300s",
                    "trigger": {"count": 1},
                },
            }
        ],
        "alertStrategy": {"autoClose": "1800s"},
    }
    response = session.post(f"{base_url}/alertPolicies", json=policy, timeout=30)
    if response.status_code in {400, 403}:
        print(
            json.dumps(
                {
                    "status": (
                        "permission_denied"
                        if response.status_code == 403
                        else "invalid_policy"
                    ),
                    "required_role": "roles/monitoring.alertPolicyEditor",
                    "response": response.json(),
                }
            ),
            file=sys.stderr,
        )
        return 3
    response.raise_for_status()
    created = response.json()
    print(json.dumps({"status": "created", "name": created["name"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
