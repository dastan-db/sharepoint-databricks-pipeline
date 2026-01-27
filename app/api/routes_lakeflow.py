from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from app.core.models import LakeflowJobConfig
from app.services.unity_catalog import UnityCatalog
from app.services.excel_sync_notebook import ExcelSyncNotebook
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import IngestionConfig, IngestionPipelineDefinition, IngestionSourceType, SchemaSpec
from databricks.sdk.service.jobs import Task, PipelineTask, NotebookTask, TaskDependency, Source
from databricks.sdk.service.workspace import ImportFormat, Language
import os
from datetime import datetime
import uuid
import base64

router = APIRouter()


def _get_lakeflow_jobs_table():
    """Get fully qualified table name for lakeflow jobs"""
    catalog = os.getenv("UC_CATALOG", "main")
    schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")
    return f"{catalog}.{schema}.lakeflow_jobs"


@router.get("/jobs")
async def list_lakeflow_jobs():
    """List all Lakeflow jobs"""
    try:
        jobs_table = _get_lakeflow_jobs_table()
        query = f"""
            SELECT connection_id, connection_name, source_schema,
                   destination_catalog, destination_schema, 
                   document_pipeline_id,
                   document_table,
                   created_at,
                   job_id,
                   tracked_file_path,
                   target_table,
                   sync_enabled
            FROM {jobs_table}
            ORDER BY created_at DESC
        """
        rows = UnityCatalog.query(query)
        
        jobs = []
        for row in rows:
            jobs.append(LakeflowJobConfig(
                connection_id=row['connection_id'],
                connection_name=row['connection_name'],
                source_schema=row['source_schema'],
                destination_catalog=row['destination_catalog'],
                destination_schema=row['destination_schema'],
                document_pipeline_id=row['document_pipeline_id'],
                document_table=row['document_table'],
                created_at=row['created_at'],
                job_id=row.get('job_id'),
                tracked_file_path=row.get('tracked_file_path'),
                target_table=row.get('target_table'),
                sync_enabled=bool(row.get('sync_enabled', False)),
            ))
        return jobs
    except Exception as e:
        return []


@router.post("/jobs")
async def create_lakeflow_job(config: LakeflowJobConfig):
    """Create a new Lakeflow ingestion job with two pipelines"""
    # Validate that source_schema (site_id) is not empty
    if not config.source_schema or config.source_schema.strip() == "":
        raise HTTPException(
            status_code=400, 
            detail="SharePoint Site ID is required. Please enter the Site ID for the SharePoint site you want to ingest from."
        )
    
    try:
        jobs_table = _get_lakeflow_jobs_table()
        catalog = os.getenv("UC_CATALOG", "main")
        schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")
        
        # Ensure schema exists
        try:
            UnityCatalog.query(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        except Exception as schema_err:
            print(f"Schema creation note: {schema_err}")  # May already exist
        
        # Create table if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {jobs_table} (
                connection_id STRING PRIMARY KEY,
                connection_name STRING NOT NULL,
                source_schema STRING NOT NULL,
                destination_catalog STRING NOT NULL,
                destination_schema STRING NOT NULL,
                document_pipeline_id STRING,
                document_table STRING,
                created_at TIMESTAMP,
                job_id STRING,
                tracked_file_path STRING,
                target_table STRING,
                sync_enabled BOOLEAN
            )
        """
        UnityCatalog.query(create_table_query)
        
        # Create destination schema if it doesn't exist
        try:
            UnityCatalog.query(f"CREATE SCHEMA IF NOT EXISTS {config.destination_catalog}.{config.destination_schema}")
        except Exception as schema_err:
            print(f"Schema creation note: {schema_err}")
        
        # Set fully qualified table name
        config.document_table = f"{config.destination_catalog}.{config.destination_schema}.documents"
        
        # Create the Databricks workspace client
        w = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        )
        
        # Generate unique pipeline name
        unique_id = str(uuid.uuid4())[:8]
        
        # Create single document ingestion pipeline
        doc_ingestion_def = IngestionPipelineDefinition(
            connection_name=config.connection_name,
            source_type=IngestionSourceType.SHAREPOINT,
            objects=[IngestionConfig(
                schema=SchemaSpec(
                    source_schema=config.source_schema,
                    destination_catalog=config.destination_catalog,
                    destination_schema=config.destination_schema
                )
            )]
        )
        
        # Create document pipeline via Databricks API
        # NOTE: Lakeflow ingestion pipelines manage their own compute - cannot use cluster_id
        # Instead, use development mode (non-continuous) for faster iterations
        pipeline_params = {
            "name": f"{config.connection_name}_docs_{unique_id}",
            "ingestion_definition": doc_ingestion_def,
            "target": config.destination_schema,
            "channel": "PREVIEW",
            "catalog": config.destination_catalog,
            "continuous": False,  # Development mode - faster for testing, runs on-demand
            "development": True   # Use development mode for faster startup
        }
        
        doc_pipeline = w.pipelines.create(**pipeline_params)
        config.document_pipeline_id = doc_pipeline.pipeline_id
        config.created_at = datetime.utcnow().isoformat()
        
        # Create Databricks Job that wraps the pipeline with a downstream sync task
        # The sync task is initially a placeholder - configured later via /configure-sync
        notebook_path = ExcelSyncNotebook.get_notebook_path(config.connection_id)
        
        # Create placeholder notebook so job doesn't fail before sync is configured
        parent_dir = "/".join(notebook_path.split("/")[:-1])
        try:
            w.workspace.mkdirs(path=parent_dir)
            placeholder_code = """# Databricks notebook source
# MAGIC %md
# MAGIC # Placeholder Notebook
# MAGIC 
# MAGIC This notebook is a placeholder until sync is configured via `/configure-sync`.
# MAGIC It will be replaced with the actual sync notebook when you configure auto-sync.

# COMMAND ----------

print("Sync not yet configured. Please configure sync via the UI.")
dbutils.notebook.exit("SYNC_NOT_CONFIGURED")
"""
            w.workspace.import_(
                path=notebook_path,
                content=base64.b64encode(placeholder_code.encode()).decode(),
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True
            )
        except Exception as placeholder_err:
            print(f"Warning: Could not create placeholder notebook: {placeholder_err}")
        
        job = w.jobs.create(
            name=f"{config.connection_name}_sync_job_{unique_id}",
            tasks=[
                Task(
                    task_key="ingest_sharepoint",
                    description="Ingest documents from SharePoint to Delta table",
                    pipeline_task=PipelineTask(
                        pipeline_id=config.document_pipeline_id
                    )
                ),
                Task(
                    task_key="sync_excel",
                    description="Sync tracked Excel file to Delta table (CDC)",
                    depends_on=[TaskDependency(task_key="ingest_sharepoint")],
                    notebook_task=NotebookTask(
                        notebook_path=notebook_path,
                        source=Source.WORKSPACE
                    ),
                    # Task is disabled until sync is configured
                    disable_auto_optimization=True
                )
            ],
            max_concurrent_runs=1
        )
        config.job_id = str(job.job_id)
        
        # Store job config (set sync_enabled to false explicitly)
        insert_query = f"""
            INSERT INTO {jobs_table}
            (connection_id, connection_name, source_schema,
             destination_catalog, destination_schema, 
             document_pipeline_id,
             document_table,
             created_at,
             job_id,
             tracked_file_path,
             target_table,
             sync_enabled)
            VALUES ('{config.connection_id}', '{config.connection_name}', '{config.source_schema}',
                    '{config.destination_catalog}', '{config.destination_schema}',
                    '{config.document_pipeline_id}',
                    '{config.document_table}',
                    CURRENT_TIMESTAMP(),
                    '{config.job_id}',
                    NULL,
                    NULL,
                    CAST(false AS BOOLEAN))
        """
        UnityCatalog.query(insert_query)
        
        # Start the document pipeline update to begin ingestion
        doc_update = w.pipelines.start_update(pipeline_id=config.document_pipeline_id)
        
        result = {
            "message": "Lakeflow job created successfully",
            "connection_id": config.connection_id,
            "document_pipeline_id": config.document_pipeline_id,
            "document_table": config.document_table,
            "document_update_id": doc_update.update_id,
            "job_id": config.job_id
        }
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create Lakeflow job: {str(e)}")


@router.get("/jobs/{connection_id}/status")
async def get_lakeflow_job_status(connection_id: str):
    """Get deployment status of a Lakeflow job's pipeline"""
    try:
        jobs_table = _get_lakeflow_jobs_table()
        query = f"""
            SELECT document_pipeline_id, destination_catalog, destination_schema
            FROM {jobs_table}
            WHERE connection_id = '{connection_id}'
        """
        rows = UnityCatalog.query(query)
        
        if not rows:
            raise HTTPException(status_code=404, detail="Job not found")
        
        doc_pipeline_id = rows[0]['document_pipeline_id']
        dest_catalog = rows[0]['destination_catalog']
        dest_schema = rows[0]['destination_schema']
        
        w = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        )
        
        # Get document pipeline status
        doc_pipeline = w.pipelines.get(pipeline_id=doc_pipeline_id)
        
        # Get latest update - iterate through the response properly
        doc_status = {
            "state": doc_pipeline.state.value if doc_pipeline.state else "UNKNOWN",
            "latest_update": None
        }
        
        try:
            # list_updates returns a Page object, we need to iterate it properly
            updates_iter = w.pipelines.list_updates(pipeline_id=doc_pipeline_id, max_results=1)
            first_update = next(iter(updates_iter), None)
            
            if first_update:
                doc_status["latest_update"] = {
                    "update_id": first_update.update_id,
                    "state": first_update.state.value if first_update.state else "UNKNOWN",
                }
        except Exception as update_err:
            # If we can't get updates, just return the pipeline state
            print(f"Warning: Could not get pipeline updates: {update_err}")
        
        return {
            "connection_id": connection_id,
            "document_pipeline": doc_status,
            "catalog": dest_catalog,
            "schema": dest_schema
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job status: {str(e)}")


@router.get("/jobs/{connection_id}/documents")
async def get_documents_table(connection_id: str, limit: int = 100):
    """Query the documents table for a Lakeflow job"""
    try:
        jobs_table = _get_lakeflow_jobs_table()
        query = f"""
            SELECT destination_catalog, destination_schema, document_table
            FROM {jobs_table}
            WHERE connection_id = '{connection_id}'
        """
        rows = UnityCatalog.query(query)
        
        if not rows:
            raise HTTPException(status_code=404, detail="Job not found")
        
        catalog = rows[0]['destination_catalog']
        schema = rows[0]['destination_schema']
        doc_table = rows[0]['document_table']
        
        # Query the documents table
        # Note: document_table is already fully qualified (catalog.schema.table)
        full_table_name = doc_table
        docs_query = f"""
            SELECT 
                file_id as path,
                file_metadata.name as name,
                file_metadata.size_in_bytes as size,
                file_metadata.last_modified_timestamp as modificationTime,
                file_metadata.created_timestamp as createdTime,
                is_deleted
            FROM {full_table_name}
            WHERE is_deleted = false
            ORDER BY file_metadata.last_modified_timestamp DESC
            LIMIT {limit}
        """
        
        try:
            doc_rows = UnityCatalog.query(docs_query)
        except Exception as e:
            # Table might not exist yet
            return {
                "connection_id": connection_id,
                "table": full_table_name,
                "documents": [],
                "message": "Table not yet created or no documents ingested"
            }
        
        documents = []
        for row in doc_rows:
            documents.append({
                "path": row['path'],
                "name": row['name'],
                "size": row['size'],
                "modification_time": str(row['modificationTime']) if row.get('modificationTime') else None,
                "file_id": row.get('file_id'),
                "is_deleted": row.get('is_deleted', False)
            })
        
        return {
            "connection_id": connection_id,
            "table": full_table_name,
            "documents": documents,
            "count": len(documents)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query documents: {str(e)}")


@router.delete("/jobs/{connection_id}")
async def delete_lakeflow_job(connection_id: str):
    """Delete a Lakeflow job and associated Databricks resources"""
    try:
        jobs_table = _get_lakeflow_jobs_table()
        
        # Get job details first to clean up Databricks resources
        get_query = f"""
            SELECT job_id, document_pipeline_id
            FROM {jobs_table}
            WHERE connection_id = '{connection_id}'
        """
        rows = UnityCatalog.query(get_query)
        
        if rows:
            job_id = rows[0].get('job_id')
            pipeline_id = rows[0].get('document_pipeline_id')
            
            w = WorkspaceClient(
                host=os.getenv("DATABRICKS_HOST"),
                token=os.getenv("DATABRICKS_TOKEN")
            )
            
            # Delete the Databricks Job
            if job_id:
                try:
                    w.jobs.delete(job_id=int(job_id))
                except Exception as e:
                    print(f"Warning: Could not delete job {job_id}: {e}")
            
            # Delete the pipeline
            if pipeline_id:
                try:
                    w.pipelines.delete(pipeline_id=pipeline_id)
                except Exception as e:
                    print(f"Warning: Could not delete pipeline {pipeline_id}: {e}")
            
            # Try to delete the sync notebook
            try:
                notebook_path = ExcelSyncNotebook.get_notebook_path(connection_id)
                w.workspace.delete(path=notebook_path)
            except Exception as e:
                print(f"Warning: Could not delete notebook: {e}")
        
        # Delete from database
        query = f"DELETE FROM {jobs_table} WHERE connection_id = '{connection_id}'"
        UnityCatalog.query(query)
        return {"message": "Lakeflow job deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")


# ============================================
# Sync Configuration Endpoint
# ============================================

class ConfigureSyncRequest(BaseModel):
    """Request model for configuring Excel sync"""
    file_path: str  # file_id in documents table
    table_name: str  # Target table name (will be created in destination schema)
    header_row: int = 0  # Which row contains headers (0-indexed)
    selected_columns: Optional[List[str]] = None  # Columns to include (None = all)


@router.post("/jobs/{connection_id}/configure-sync")
async def configure_sync(connection_id: str, request: ConfigureSyncRequest):
    """
    Configure auto-sync for an Excel file to a Delta table.
    
    This endpoint:
    1. Generates a sync notebook with CDC logic
    2. Uploads the notebook to the Databricks workspace
    3. Updates the job configuration to enable the sync task
    4. Stores the tracked file and target table in the database
    """
    try:
        jobs_table = _get_lakeflow_jobs_table()
        
        # Get job details
        get_query = f"""
            SELECT job_id, document_table, destination_catalog, destination_schema
            FROM {jobs_table}
            WHERE connection_id = '{connection_id}'
        """
        rows = UnityCatalog.query(get_query)
        
        if not rows:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job_id = rows[0].get('job_id')
        document_table = rows[0]['document_table']
        dest_catalog = rows[0]['destination_catalog']
        dest_schema = rows[0]['destination_schema']
        
        if not job_id:
            raise HTTPException(
                status_code=400, 
                detail="Job does not have a Databricks Job ID. Please recreate the job."
            )
        
        # Build fully qualified target table name
        target_table = f"{dest_catalog}.{dest_schema}.{request.table_name}"
        
        # Generate sync notebook code
        notebook_code = ExcelSyncNotebook.generate_sync_notebook(
            document_table=document_table,
            tracked_file_path=request.file_path,
            target_table=target_table,
            header_row=request.header_row,
            selected_columns=request.selected_columns,
        )
        
        # Upload notebook to workspace
        w = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        )
        
        notebook_path = ExcelSyncNotebook.get_notebook_path(connection_id)
        
        # Ensure parent directory exists
        parent_dir = "/".join(notebook_path.split("/")[:-1])
        try:
            w.workspace.mkdirs(path=parent_dir)
        except Exception as e:
            print(f"Directory may already exist: {e}")
        
        # Import notebook (overwrite if exists)
        w.workspace.import_(
            path=notebook_path,
            content=base64.b64encode(notebook_code.encode()).decode(),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True
        )
        
        # Escape values for SQL
        escaped_file_path = request.file_path.replace("'", "''")
        escaped_target_table = target_table.replace("'", "''")
        
        # Update job configuration in database
        update_query = f"""
            UPDATE {jobs_table}
            SET tracked_file_path = '{escaped_file_path}',
                target_table = '{escaped_target_table}',
                sync_enabled = true
            WHERE connection_id = '{connection_id}'
        """
        UnityCatalog.query(update_query)
        
        return {
            "message": "Sync configured successfully",
            "connection_id": connection_id,
            "tracked_file_path": request.file_path,
            "target_table": target_table,
            "notebook_path": notebook_path,
            "job_id": job_id,
            "sync_enabled": True
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to configure sync: {str(e)}")


@router.post("/jobs/{connection_id}/run-sync")
async def run_sync_job(connection_id: str):
    """
    Manually trigger the sync job to run.
    This runs both the SharePoint ingestion and the Excel sync task.
    """
    try:
        jobs_table = _get_lakeflow_jobs_table()
        
        # Get job details
        get_query = f"""
            SELECT job_id, sync_enabled
            FROM {jobs_table}
            WHERE connection_id = '{connection_id}'
        """
        rows = UnityCatalog.query(get_query)
        
        if not rows:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job_id = rows[0].get('job_id')
        sync_enabled = rows[0].get('sync_enabled', False)
        
        if not job_id:
            raise HTTPException(
                status_code=400, 
                detail="Job does not have a Databricks Job ID"
            )
        
        if not sync_enabled:
            raise HTTPException(
                status_code=400,
                detail="Sync is not configured. Please configure sync first using /configure-sync"
            )
        
        # Trigger the job
        w = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        )
        
        run = w.jobs.run_now(job_id=int(job_id))
        
        return {
            "message": "Sync job triggered successfully",
            "connection_id": connection_id,
            "job_id": job_id,
            "run_id": run.run_id
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run sync job: {str(e)}")


@router.delete("/jobs/{connection_id}/disable-sync")
async def disable_sync(connection_id: str):
    """Disable auto-sync for a job"""
    try:
        jobs_table = _get_lakeflow_jobs_table()
        
        # Update database to disable sync
        update_query = f"""
            UPDATE {jobs_table}
            SET sync_enabled = false
            WHERE connection_id = '{connection_id}'
        """
        UnityCatalog.query(update_query)
        
        return {
            "message": "Sync disabled successfully",
            "connection_id": connection_id,
            "sync_enabled": False
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to disable sync: {str(e)}")
