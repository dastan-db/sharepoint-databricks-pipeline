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
    lakebase_table: str  # Target Lakebase documents table
    file_pattern: str = "*.xlsx"  # Filter for Excel files only
    pipeline_id: Optional[str] = None  # Databricks pipeline ID (populated after creation)


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
