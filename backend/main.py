from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.rag import router as rag_router
from backend.api.users import auth_router, users_router
from backend.db.connection import initialize_schema_if_configured


app = FastAPI(
    title="Student Mental Health Analytics API",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_origin_regex=r"^http://(localhost|127\.0\.0\.1):\d+$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(rag_router)
app.include_router(auth_router)
app.include_router(users_router)


@app.on_event("startup")
def initialize_database() -> None:
    try:
        initialize_schema_if_configured()
    except Exception as exc:
        print(f"User database initialization failed: {exc}")


@app.get("/health")
def health_check():
    return {"status": "ok"}
