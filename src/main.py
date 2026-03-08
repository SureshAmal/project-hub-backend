from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os
from src.core.config import settings

import src.models # Initialize SQLAlchemy models before request handlers are mapped
from src.api.routes import api_router
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="Python FastAPI backend port for Project Hub.",
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# Set all CORS enabled origins
if settings.cors_origins or settings.BACKEND_CORS_ORIGIN_REGEX:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_origin_regex=settings.BACKEND_CORS_ORIGIN_REGEX,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(api_router, prefix=settings.API_V1_STR)

# Serve static uploads
os.makedirs("uploads/profiles", exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

@app.get("/")
def read_root():
    return {"message": "Welcome to Project Hub API!"}
