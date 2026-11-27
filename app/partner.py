from fastapi import FastAPI

from app.routers.partner import router

app = FastAPI(title="Fake Webhook Partner", version="0.1.0")
app.include_router(router)
