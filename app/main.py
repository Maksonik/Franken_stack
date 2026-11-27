from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routers.lab import LAB_FILES
from app.routers.lab import router as lab_router
from app.routers.payments import router

app = FastAPI(title="Payments Lab", version="0.1.0")
app.include_router(router)
app.include_router(lab_router)
app.mount("/lab/assets", StaticFiles(directory=LAB_FILES), name="lab_assets")
