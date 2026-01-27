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


class SharePointConnection(BaseModel):
    id: str
    name: str
    client_id: str
    client_secret: str
    tenant_id: str
    refresh_token: str
    site_id: str
    connection_name: str = ""  # Unity Catalog connection name (generated)


class SharePointPipelineConfig(BaseModel):
    id: str
    name: str
    connection_id: str
    ingestion_type: Literal["all_drives", "specific_drives"]
    drive_names: Optional[List[str]] = None
    delta_table: str  # Target Unity Catalog Delta table
    file_pattern: str = "*.xlsx"  # Filter for Excel files only
    pipeline_id: Optional[str] = None  # Databricks pipeline ID (populated after creation)


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


class ExcelStreamConfig(BaseModel):
    id: str
    name: str
    lakebase_table: str  # Source: Lakebase documents table
    file_name_pattern: str  # e.g., "supplier_*.xlsx"
    destination_catalog: str
    destination_schema: str
    destination_table: str
    checkpoint_location: str  # For streaming state
    trigger_interval: str = "10 seconds"  # Streaming trigger frequency
    is_active: bool = False  # Enable/disable streaming
