SELF_HARM_KEYWORDS = (
    "suicide",
    "kill myself",
    "end my life",
    "self harm",
    "self-harm",
    "hurt myself",
    "can't go on",
    "cannot go on",
)

HARM_TO_OTHERS_KEYWORDS = (
    "kill someone",
    "kill somebody",
    "kill him",
    "kill her",
    "kill them",
    "hurt someone",
    "hurt somebody",
    "hurt him",
    "hurt her",
    "hurt them",
    "harm someone",
    "harm somebody",
    "attack someone",
    "attack somebody",
    "murder",
)


CRISIS_MESSAGE = (
    "\n\nIf you or someone else may be in immediate danger, contact local emergency services now. "
    "In the U.S. or Canada, call or text 988 for crisis support. If you are outside those regions, "
    "contact your local emergency number, campus security, or a trusted crisis hotline."
)


DISCLAIMER = (
    "\n\nNote: I can offer general support, but I cannot replace a counselor, clinician, "
    "emergency responder, or trusted person who can be with you in real life."
)


def has_crisis_signal(text: str) -> bool:
    return detect_crisis_type(text) is not None


def detect_crisis_type(text: str) -> str | None:
    normalized = text.lower()
    if any(keyword in normalized for keyword in HARM_TO_OTHERS_KEYWORDS):
        return "harm_to_others"
    if any(keyword in normalized for keyword in SELF_HARM_KEYWORDS):
        return "self_harm"
    return None


def apply_safety_guardrails(answer: str, question: str) -> str:
    guarded = answer.strip()
    if DISCLAIMER.lower() not in guarded.lower():
        guarded += DISCLAIMER
    if has_crisis_signal(question):
        guarded += CRISIS_MESSAGE
    return guarded
