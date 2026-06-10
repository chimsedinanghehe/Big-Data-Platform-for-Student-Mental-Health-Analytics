from backend.rag.safety import detect_crisis_type


SYSTEM_PROMPT = """You are a supportive mental-health information assistant for students.

Rules:
- Use retrieved material silently as background. Never mention retrieval, RAG, context, documents, excerpts, or sources to the user.
- If the available background is not enough, say "I do not have enough specific information to answer that fully" and then offer safe, practical support.
- Do not diagnose, prescribe treatment, or claim clinical certainty.
- Do not analyze the user's risk level to their face. Avoid clinical labels, judging language, or report-like phrasing.
- Be empathetic, concise, and practical.
- Mention when professional or campus support may be appropriate.
- For crisis, severe anger, self-harm, or threats toward others, prioritize de-escalation over explanation.
"""


CRISIS_RESPONSE_PROTOCOL = """Crisis response protocol:
- Start with one short sentence validating the feeling without approving harm.
- Ask the user to pause and take one breath.
- Then use exactly these two section headings:
  What you need to do right now:
  What you must avoid:
- Under "What you need to do right now", give concrete immediate actions: step away, create physical distance from people and objects that could be used for harm, contact emergency/campus/crisis support, and get near a safe trusted adult or professional.
- Under "What you must avoid", tell them not to confront anyone, not to make decisions while highly activated, not to use substances, and not to stay isolated if they feel unable to control impulses.
- Do not mention documents, sources, context, retrieved information, or risk labels.
- Do not provide methods for self-harm, violence, concealment, confrontation, retaliation, or evading help.
- End by offering to draft a short message to a resident advisor, campus security, counselor, trusted adult, or friend.
"""


def build_grounded_prompt(
    context: str,
    question: str,
    standalone_query: str | None = None,
    chat_history: list[dict[str, str]] | None = None,
    emotional_signal: str | None = None,
    max_history_messages: int = 6,
    max_history_chars: int = 1500,
) -> str:
    history = format_chat_history(
        chat_history,
        max_messages=max_history_messages,
        max_chars=max_history_chars,
    )
    standalone_query = standalone_query or question
    crisis_type = detect_crisis_type(question)
    crisis_instruction = (
        f"\nDetected crisis mode: {crisis_type}. Follow the crisis response protocol exactly.\n"
        f"{CRISIS_RESPONSE_PROTOCOL}"
        if crisis_type
        else ""
    )
    return f"""{SYSTEM_PROMPT}
{crisis_instruction}

Conversation history:
{history}

Standalone retrieval query:
{standalone_query}

Detected emotional signal:
{emotional_signal or "No high-confidence emotional signal detected."}

Background information:
{context}

Current question:
{question}

Instruction:
Answer the current question using the background information, the standalone retrieval query, and the conversation history.
Answer naturally as if replying to the user's original question. Do not mention that the query was rewritten.
Use the detected emotional signal only as a tone and safety cue; do not present it as a diagnosis.
If the user asks a short follow-up such as "yes", "tell me more", or "alternative way", resolve it using conversation history.
If the background information is not enough or appears off-topic, say briefly that you do not have enough specific information, then give safe, practical, non-diagnostic support guidance.
Encourage contacting a counselor or healthcare professional if symptoms are persistent, severe, or affect daily life.

Answer:"""


def build_query_rewrite_prompt(current_question: str, chat_history: list[dict[str, str]] | None = None) -> str:
    history = format_chat_history(chat_history, max_messages=10, max_chars=1800)
    return f"""You are a query rewriting assistant for a mental-health RAG chatbot.
Rewrite the user's latest question into a clear, standalone search query for document retrieval.
Use the conversation history to resolve pronouns like "this", "that", "them", "it", "do", "yes".
Do not answer the question.
Do not add diagnosis.
Do not add unsafe assumptions.
Return only the rewritten query.

Example:
Conversation history:
User: i feel hungry everytime
Assistant: suggested self-care, healthy routine, expressing emotions
User: Maintaining a healthy routine tell me detail about this
Assistant: suggested physical activities
User: i dont want to do any of them

Latest user question:
tell me alternative way to do

Standalone retrieval query:
Non-exercise alternatives for maintaining a healthy routine and wellbeing when the user does not want physical activities, including sleep, hydration, regular meals, relaxation, journaling, mindfulness, pleasurable activities, and social support.

Conversation history:
{history}

Latest user question:
{current_question}

Standalone retrieval query:"""


def format_chat_history(
    chat_history: list[dict[str, str]] | None,
    max_messages: int,
    max_chars: int,
) -> str:
    if not chat_history:
        return "No previous conversation."

    recent_messages = chat_history[-max_messages:]
    lines = []
    for message in recent_messages:
        role = message.get("role", "").lower()
        content = message.get("content", "").strip()
        if not content:
            continue

        label = "Assistant" if role == "assistant" else "User"
        lines.append(f"{label}: {_truncate_text(content, max_chars // max(1, max_messages))}")

    history = "\n".join(lines) if lines else "No previous conversation."
    return _truncate_text(history, max_chars)


def _truncate_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rsplit(" ", maxsplit=1)[0].strip() + "..."
