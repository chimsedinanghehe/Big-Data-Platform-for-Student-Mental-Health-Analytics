from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    role: str
    content: str


class RAGAskRequest(BaseModel):
    question: str | None = Field(default=None)
    session_id: str | None = Field(default=None)
    chat_history: list[ChatMessage] = Field(default_factory=list)


class RAGAskResponse(BaseModel):
    answer: str
    session_id: str


class ErrorDetail(BaseModel):
    error: str
    message: str


class ErrorResponse(BaseModel):
    detail: ErrorDetail
