from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers.partner import router

app = FastAPI(title="Fake Webhook Partner", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000", "http://127.0.0.1:8000"],
    allow_methods=["POST"],
)
app.include_router(router)
