from fastapi import FastAPI

from app.routers.payments import router

app = FastAPI(title="Payments Lab", version="0.1.0")
app.include_router(router)
