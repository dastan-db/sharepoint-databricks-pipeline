# app/core/models.py
from pydantic import BaseModel
from typing import Dict, Any, Literal, Optional, List


class SyncConfig(BaseModel):
    id: str
    catalog: str
    schema_name: str
    file_name: str
    documents_table: str = "documents"
    target_table: str = "supplier_a_data"


class RunResult(BaseModel):
    status: Literal["skipped", "success", "dq_failed", "error"]
    gate: Dict[str, Any]
    parse: Optional[Dict[str, Any]] = None
    dq: Optional[Dict[str, Any]] = None


class LakeflowJobConfig(BaseModel):
    connection_id: str  # Reference to SharePoint connection (PRIMARY KEY)
    connection_name: str  # Unity Catalog connection name (e.g., "sharepoint-fe")
    source_schema: str  # SharePoint site ID (UUID)
    destination_catalog: str  # Unity Catalog (e.g., "main")
    destination_schema: str  # Schema where tables will land
    document_pipeline_id: Optional[str] = None  # Document ingestion pipeline
    document_table: Optional[str] = None  # Full path: {catalog}.{schema}.documents
    created_at: Optional[str] = None
    # Databricks Job fields for downstream sync task
    job_id: Optional[str] = None  # Databricks Job ID (wraps pipeline + sync task)
    tracked_file_path: Optional[str] = None  # Excel file to sync (file_id in documents table)
    target_table: Optional[str] = None  # Destination Delta table for parsed Excel data
    sync_enabled: bool = False  # Whether sync task is configured and enabled
