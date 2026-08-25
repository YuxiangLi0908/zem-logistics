from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import health, navigation


app = FastAPI(
    title="ZEM Logistics API",
    version="0.1.0",
    description="Future FastAPI service for the Vue frontend.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/api")
app.include_router(navigation.router, prefix="/api")
