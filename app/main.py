# app/main.py
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from app.api.routes_runs import router as runs_router
from app.api.routes_config import router as config_router
from app.api.routes_sharepoint import router as sharepoint_router
from app.api.routes_excel_streaming import router as excel_streaming_router
from app.api.routes_lakeflow import router as lakeflow_router
from app.api.routes_excel import router as excel_router
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
        
        # Initialize SharePoint tables in Unity Catalog
        sharepoint_result = await SchemaManager.initialize_sharepoint_tables()
        for table_name, status in sharepoint_result.items():
            status_str = "✓" if status else "✗"
            print(f"  {status_str} {table_name}")
        
        # Initialize Lakebase tables (tracked but managed via Lakebase service)
        lakebase_result = await SchemaManager.initialize_lakebase_tables()
        for table_name, status in lakebase_result.items():
            status_str = "✓" if status else "✗"
            print(f"  {status_str} {table_name} (Lakebase/PostgreSQL)")
        
        print("Database schema initialization complete.")
    except Exception as e:
        print(f"Warning: Failed to initialize database schema: {str(e)}")
        print("Application will continue, but some features may not work correctly.")


app.include_router(runs_router, prefix="/runs", tags=["runs"])
app.include_router(config_router, prefix="/configs", tags=["configs"])
app.include_router(sharepoint_router, prefix="/sharepoint", tags=["sharepoint"])
app.include_router(excel_streaming_router, prefix="/excel-streaming", tags=["excel-streaming"])
app.include_router(lakeflow_router, prefix="/api/lakeflow", tags=["lakeflow"])
app.include_router(excel_router, prefix="/api/excel", tags=["excel"])


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def root():
    """Serve the main UI."""
    index_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "index.html")
    return FileResponse(index_path)
