# app/main.py
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from app.api.routes_runs import router as runs_router
from app.api.routes_config import router as config_router
from app.api.routes_sharepoint import router as sharepoint_router
from app.api.routes_excel_streaming import router as excel_streaming_router
import os

app = FastAPI(title="SharePoint to Databricks Data Pipeline")

app.include_router(runs_router, prefix="/runs", tags=["runs"])
app.include_router(config_router, prefix="/configs", tags=["configs"])
app.include_router(sharepoint_router, prefix="/sharepoint", tags=["sharepoint"])
app.include_router(excel_streaming_router, prefix="/excel-streaming", tags=["excel-streaming"])


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def root():
    """Serve the main UI."""
    index_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "index.html")
    return FileResponse(index_path)
