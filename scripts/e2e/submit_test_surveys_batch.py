from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time
from pathlib import Path
from uuid import uuid4

from submit_test_survey import pick_option, request_json


CHAT_QUESTIONS = [
    "What are two practical ways to manage study stress this week?",
    "How can a student build a healthier sleep routine?",
    "What should I do when schoolwork feels overwhelming?",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Register test students, submit one survey per account, and optionally send chat messages."
    )
    parser.add_argument("--api-base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--count", type=int, default=25)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--chat-messages-per-user", type=int, default=0, choices=range(0, 4))
    parser.add_argument("--password", default="SurveyE2E123!")
    parser.add_argument("--email-prefix", default="survey.e2e.batch")
    parser.add_argument("--output-json")
    return parser.parse_args()


def student_profile(index: int) -> dict:
    school_ages = [14, 15, 16, 17, 18]
    university_ages = [19, 20, 21, 22, 23, 24]
    learner_type = "high_school" if index % 2 == 0 else "university"
    ages = school_ages if learner_type == "high_school" else university_ages
    genders = ["female", "male", "other"]
    return {
        "age": ages[index % len(ages)],
        "gender": genders[index % len(genders)],
        "learner_type": learner_type,
    }


def submit_one(base_url: str, index: int, args: argparse.Namespace) -> dict:
    stamp = time.strftime("%Y%m%d%H%M%S")
    profile = student_profile(index)
    email = f"{args.email_prefix}.{stamp}.{index:03d}@example.com"
    register_payload = {
        "email": email,
        "password": args.password,
        "display_name": f"Survey E2E Batch {index:03d}",
        "role": "student",
        "student_profile": profile,
    }
    auth = request_json("POST", f"{base_url}/api/auth/register", register_payload)
    token = auth["access_token"]

    questions_payload = request_json("GET", f"{base_url}/api/survey/questions", token=token)
    answers = {
        question["id"]: pick_option(
            question,
            age=profile["age"],
            gender=profile["gender"],
            learner_type=profile["learner_type"],
        )
        for question in questions_payload.get("questions", [])
    }
    submit = request_json(
        "POST",
        f"{base_url}/api/survey/submit",
        {
            "survey_type": questions_payload.get("survey_type"),
            "answers": answers,
        },
        token=token,
    )
    session_id = f"survey-e2e-{uuid4()}"
    chat_results = []
    for message_index in range(args.chat_messages_per_user):
        question = CHAT_QUESTIONS[(index + message_index) % len(CHAT_QUESTIONS)]
        chat_result = request_json(
            "POST",
            f"{base_url}/api/rag/ask",
            {
                "session_id": session_id,
                "question": question,
                "chat_history": [],
            },
            token=token,
        )
        chat_results.append(
            {
                "question": question,
                "session_id": chat_result.get("session_id"),
                "answer_length": len(str(chat_result.get("answer") or "")),
            }
        )
    return {
        "index": index,
        "email": email,
        "user_id": auth["user"]["id"],
        "profile": profile,
        "survey_type": questions_payload.get("survey_type"),
        "question_count": len(questions_payload.get("questions", [])),
        "answer_count": len(answers),
        "submit_status": submit.get("status", {}),
        "chat_messages_requested": args.chat_messages_per_user,
        "chat_messages_sent": len(chat_results),
        "chat_results": chat_results,
    }


def main() -> int:
    args = parse_args()
    base_url = args.api_base_url.rstrip("/")
    health = request_json("GET", f"{base_url}/health")
    started_at = time.perf_counter()
    results = []
    failures = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(submit_one, base_url, index, args): index
            for index in range(1, args.count + 1)
        }
        for future in as_completed(futures):
            index = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                failures.append({"index": index, "error": str(exc)})
    results.sort(key=lambda item: item["index"])
    failures.sort(key=lambda item: item["index"])
    payload = {
        "api_base_url": base_url,
        "health": health,
        "requested_count": args.count,
        "submitted_count": len(results),
        "failed_count": len(failures),
        "chat_messages_per_user": args.chat_messages_per_user,
        "chat_messages_sent": sum(item["chat_messages_sent"] for item in results),
        "workers": max(1, args.workers),
        "duration_seconds": round(time.perf_counter() - started_at, 3),
        "failures": failures,
        "results": results,
    }
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
