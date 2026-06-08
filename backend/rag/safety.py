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
    "\n\nNếu bạn hoặc người khác có thể đang gặp nguy hiểm ngay lúc này, hãy liên hệ dịch vụ khẩn cấp tại nơi bạn sống. "
    "Nếu ở Mỹ hoặc Canada, bạn có thể gọi hoặc nhắn 988 để được hỗ trợ khủng hoảng. Nếu ở khu vực khác, "
    "hãy liên hệ số khẩn cấp địa phương, bảo vệ trường, chuyên viên tư vấn hoặc một người lớn đáng tin."
)


DISCLAIMER = (
    "\n\nLưu ý: Mình có thể hỗ trợ thông tin và gợi ý chung, nhưng không thay thế chuyên viên tư vấn, bác sĩ, "
    "người ứng cứu khẩn cấp hoặc một người đáng tin có thể ở cạnh bạn ngoài đời."
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
