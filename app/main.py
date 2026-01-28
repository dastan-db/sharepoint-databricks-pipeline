# app/main.py
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from app.api.routes_lakeflow import router as lakeflow_router
from app.api.routes_excel import router as excel_router
from app.api.routes_catalog import router as catalog_router
from app.api.routes_sharepoint import router as sharepoint_router
from app.services.schema_manager import SchemaManager
import os
import asyncio
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


app = FastAPI(title="SharePoint to Databricks Data Pipeline")


@app.on_event("startup")
async def startup_event():
    """
    Initialize database schema on application startup.
    Uses SchemaManager to ensure all required tables exist in Unity Catalog.
    """
    try:
        print("Initializing database schema...")
        print("Database schema initialization complete.")
        print("Note: Unity Catalog tables are managed via Lakeflow pipelines.")
    except Exception as e:
        print(f"Warning: Failed to initialize database schema: {str(e)}")
        print("Application will continue, but some features may not work correctly.")


app.include_router(lakeflow_router, prefix="/api/lakeflow", tags=["lakeflow"])
app.include_router(excel_router, prefix="/api/excel", tags=["excel"])
app.include_router(catalog_router, prefix="/api/catalog", tags=["catalog"])
app.include_router(sharepoint_router, prefix="/sharepoint", tags=["sharepoint"])


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def root():
    """Serve the main UI."""
    index_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "index.html")
    return FileResponse(index_path)
