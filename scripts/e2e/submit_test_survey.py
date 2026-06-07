from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register a local test student and submit one valid survey response.")
    parser.add_argument("--api-base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--email-prefix", default="survey.e2e")
    parser.add_argument("--password", default="SurveyE2E123!")
    parser.add_argument("--display-name", default="Survey E2E Student")
    parser.add_argument("--age", type=int, default=17)
    parser.add_argument("--gender", default="female", choices=["male", "female", "other"])
    parser.add_argument("--learner-type", default="high_school")
    parser.add_argument("--output-json")
    return parser.parse_args()


def request_json(method: str, url: str, payload: dict | None = None, token: str | None = None) -> dict:
    headers = {"Accept": "application/json"}
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed with HTTP {exc.code}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"{method} {url} failed: {exc}") from exc


def pick_option(question: dict, *, age: int, gender: str, learner_type: str) -> str:
    input_type = question.get("input_type")
    if input_type == "date":
        return date.today().isoformat()

    options = [str(option) for option in question.get("options") or []]
    if not options:
        return ""

    qid = str(question.get("id") or "").lower()
    normalized = [(option, normalize(option)) for option in options]

    if qid == "gender":
        if gender == "male":
            return first_contains(normalized, ["nam", "male"]) or options[0]
        if gender == "female":
            return first_contains(normalized, ["nu", "female"]) or options[0]
        return first_contains(normalized, ["khac", "other", "khong muon"]) or options[-1]

    if qid == "age":
        exact = first_contains(normalized, [str(age)])
        if exact:
            return exact
        if age <= 18:
            return first_contains(normalized, ["17", "18", "16"]) or options[0]
        return first_contains(normalized, ["19", "20", "21"]) or options[0]

    if qid in {"school_grade", "grade"}:
        return first_contains(normalized, ["12", "11", "high"]) or options[0]

    if qid in {"university_year", "yr_sch"}:
        return first_contains(normalized, ["1", "first", "nam 1"]) or options[0]

    if qid == "learner_type":
        target = normalize(learner_type).replace("_", " ")
        return first_contains(normalized, [target]) or options[0]

    return options[0]


def first_contains(options: list[tuple[str, str]], tokens: list[str]) -> str | None:
    normalized_tokens = [normalize(token) for token in tokens]
    for original, normalized in options:
        if any(token in normalized for token in normalized_tokens):
            return original
    return None


def normalize(value: str) -> str:
    import unicodedata

    stripped = unicodedata.normalize("NFD", str(value))
    stripped = "".join(ch for ch in stripped if unicodedata.category(ch) != "Mn")
    return stripped.strip().lower()


def main() -> int:
    args = parse_args()
    base = args.api_base_url.rstrip("/")
    stamp = time.strftime("%Y%m%d%H%M%S")
    email = f"{args.email_prefix}.{stamp}@example.com"

    health = request_json("GET", f"{base}/health")
    register_payload = {
        "email": email,
        "password": args.password,
        "display_name": f"{args.display_name} {stamp}",
        "role": "student",
        "student_profile": {
            "age": args.age,
            "gender": args.gender,
            "learner_type": args.learner_type,
        },
    }
    auth = request_json("POST", f"{base}/api/auth/register", register_payload)
    token = auth["access_token"]

    questions_payload = request_json("GET", f"{base}/api/survey/questions", token=token)
    answers = {
        question["id"]: pick_option(
            question,
            age=args.age,
            gender=args.gender,
            learner_type=args.learner_type,
        )
        for question in questions_payload.get("questions", [])
    }
    submit = request_json(
        "POST",
        f"{base}/api/survey/submit",
        {
            "survey_type": questions_payload.get("survey_type"),
            "answers": answers,
        },
        token=token,
    )

    result = {
        "api_base_url": base,
        "health": health,
        "email": email,
        "user_id": auth["user"]["id"],
        "survey_type": questions_payload.get("survey_type"),
        "questions": len(questions_payload.get("questions", [])),
        "answers": len(answers),
        "submit_status": submit.get("status", {}),
    }

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
