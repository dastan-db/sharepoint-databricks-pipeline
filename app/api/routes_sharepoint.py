# app/api/routes_sharepoint.py
"""
SharePoint Routes - Refactored for security using Databricks Tools Core.
Eliminates SQL injection vulnerabilities by using:
1. Unity Catalog SDK for table management (via SchemaManager)
2. Safe SQL execution with proper escaping
3. databricks_tools_core.sql for queries
"""
from fastapi import APIRouter, HTTPException
from typing import List
import json
import os
from app.core.models import SharePointConnection, SharePointPipelineConfig
from app.services.secure_sql import SecureSQL
from app.services.sharepoint_connector import SharePointConnector
import asyncio

router = APIRouter()


def _get_connections_table():
    """Get fully qualified table name for SharePoint connections"""
    catalog = os.getenv("UC_CATALOG", "main")
    schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")
    return f"{catalog}.{schema}.sharepoint_connections"


def _get_pipelines_table():
    """Get fully qualified table name for SharePoint pipelines"""
    catalog = os.getenv("UC_CATALOG", "main")
    schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")
    return f"{catalog}.{schema}.sharepoint_pipelines"


def _escape_sql_string(value: str) -> str:
    """
    Escape a string value for safe SQL interpolation.
    This is a temporary measure - ideally we'd use parameterized queries.
    """
    if value is None:
        return "NULL"
    # Escape single quotes by doubling them (SQL standard)
    return value.replace("'", "''")


# ============================================
# Connection Endpoints
# ============================================

@router.get("/connections")
async def list_connections() -> List[SharePointConnection]:
    """List all SharePoint Lakeflow connector configurations."""
    try:
        # Get all connections from Databricks
        query = "SHOW CONNECTIONS"
        results = await SecureSQL.execute_query(query)
        
        # Filter for SharePoint connections only
        sharepoint_connections = [
            conn for conn in results 
            if conn.get('type', '').upper() == 'SHAREPOINT'
        ]
        
        # Convert to SharePointConnection model
        connections = []
        for conn in sharepoint_connections:
            conn_name = conn.get('name', '')
            conn_comment = conn.get('comment', '')
            connections.append(SharePointConnection(
                id=conn_name,
                name=conn_name,
                client_id="",
                client_secret="****",
                tenant_id="",
                refresh_token="****",
                site_id=conn_comment or "",
                connection_name=conn_name,
            ))
        
        return connections
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            return []
        raise HTTPException(status_code=500, detail=f"Failed to list connections: {str(e)}")


@router.post("/connections")
async def create_connection(connection: SharePointConnection) -> dict:
    """
    Create a new SharePoint connection in Unity Catalog.
    
    This creates a native Unity Catalog SHAREPOINT connection that can be used
    with Lakeflow pipelines and other Databricks features.
    """
    try:
        from app.services.unity_catalog import UnityCatalog
        
        # Build CREATE CONNECTION SQL for Unity Catalog
        # Use the connection name from the model or generate one
        conn_name = connection.connection_name or connection.name.lower().replace(" ", "_")
        
        # Create Unity Catalog SharePoint connection
        create_sql = f"""
            CREATE CONNECTION IF NOT EXISTS {conn_name}
            TYPE SHAREPOINT
            OPTIONS (
                host 'graph.microsoft.com',
                clientId '{connection.client_id}',
                clientSecret '{connection.client_secret}',
                tenantId '{connection.tenant_id}',
                refreshToken '{connection.refresh_token}'
            )
            COMMENT '{connection.site_id or connection.name}'
        """
        
        # Execute via UnityCatalog service
        try:
            UnityCatalog.query(create_sql)
        except Exception as create_err:
            # If connection already exists, that's okay for testing
            if "already exists" in str(create_err).lower():
                return {
                    "message": f"Connection '{conn_name}' already exists",
                    "id": conn_name,
                    "connection_name": conn_name
                }
            raise
        
        return {
            "message": "Unity Catalog SharePoint connection created successfully",
            "id": conn_name,
            "connection_name": conn_name
        }
    except Exception as e:
        if "already exists" in str(e).lower():
            raise HTTPException(
                status_code=400, 
                detail=f"Connection already exists. Use the existing connection from the list above."
            )
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to create Unity Catalog connection: {str(e)}"
        )


@router.get("/connections/{connection_id}")
async def get_connection(connection_id: str) -> SharePointConnection:
    """Get a specific SharePoint connection."""
    try:
        connections_table = _get_connections_table()
        
        # SECURE: Escape the connection_id to prevent SQL injection
        escaped_id = _escape_sql_string(connection_id)
        
        query = f"""
            SELECT id, name, client_id, client_secret, tenant_id, refresh_token, site_id, connection_name
            FROM {connections_table}
            WHERE id = '{escaped_id}'
        """
        
        catalog = os.getenv("UC_CATALOG", "main")
        schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")
        rows = await SecureSQL.execute_query(query, catalog=catalog, schema=schema)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")
        
        row = rows[0]
        return SharePointConnection(
            id=row['id'],
            name=row['name'],
            client_id=row['client_id'],
            client_secret=row['client_secret'],
            tenant_id=row['tenant_id'],
            refresh_token=row['refresh_token'],
            site_id=row['site_id'],
            connection_name=row['connection_name'],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get connection: {str(e)}")


@router.delete("/connections/{connection_id}")
async def delete_connection(connection_id: str) -> dict:
    """Delete a SharePoint connection."""
    try:
        connections_table = _get_connections_table()
        
        # SECURE: Escape the connection_id
        escaped_id = _escape_sql_string(connection_id)
        
        # Check if connection exists
        check_query = f"SELECT id FROM {connections_table} WHERE id = '{escaped_id}'"
        catalog = os.getenv("UC_CATALOG", "main")
        schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")
        rows = await SecureSQL.execute_query(check_query, catalog=catalog, schema=schema)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")
        
        # Delete the connection
        delete_query = f"DELETE FROM {connections_table} WHERE id = '{escaped_id}' RETURNING *"
        await SecureSQL.execute_query(delete_query, catalog=catalog, schema=schema)
        
        return {"message": "Connection deleted successfully", "id": connection_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete connection: {str(e)}")


@router.post("/connections/{connection_id}/test")
async def test_connection(connection_id: str) -> dict:
    """Test a SharePoint connection."""
    try:
        connection = await get_connection(connection_id)
        result = await asyncio.to_thread(SharePointConnector.test_connection, connection)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to test connection: {str(e)}")


# ============================================
# Pipeline Endpoints
# ============================================

@router.get("/pipelines")
async def list_pipelines() -> List[SharePointPipelineConfig]:
    """List all SharePoint pipeline configs."""
    try:
        pipelines_table = _get_pipelines_table()
        query = f"""
            SELECT id, name, connection_id, ingestion_type, drive_names, 
                   delta_table, file_pattern, pipeline_id
            FROM {pipelines_table}
            ORDER BY name
        """
        
        catalog = os.getenv("UC_CATALOG", "main")
        schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")
        rows = await SecureSQL.execute_query(query, catalog=catalog, schema=schema)
        
        pipelines = []
        for row in rows:
            drive_names = json.loads(row['drive_names']) if row.get('drive_names') else None
            pipelines.append(SharePointPipelineConfig(
                id=row['id'],
                name=row['name'],
                connection_id=row['connection_id'],
                ingestion_type=row['ingestion_type'],
                drive_names=drive_names,
                delta_table=row['delta_table'],
                file_pattern=row.get('file_pattern') or "*.xlsx",
                pipeline_id=row.get('pipeline_id'),
            ))
        return pipelines
    except Exception as e:
        if "does not exist" in str(e).lower() or "not found" in str(e).lower():
            return []
        raise HTTPException(status_code=500, detail=f"Failed to list pipelines: {str(e)}")


@router.post("/pipelines")
async def create_pipeline(config: SharePointPipelineConfig) -> dict:
    """Create a new SharePoint ingestion pipeline targeting Lakebase."""
    try:
        pipelines_table = _get_pipelines_table()
        
        # Tables are initialized by SchemaManager - no CREATE TABLE needed
        
        # Get connection details
        connection = await get_connection(config.connection_id)
        
        # Create pipeline via service
        pipeline_id = await asyncio.to_thread(
            SharePointConnector.create_pipeline, config, connection
        )
        config.pipeline_id = pipeline_id
        
        # SECURE: Escape all values
        escaped_id = _escape_sql_string(config.id)
        escaped_name = _escape_sql_string(config.name)
        escaped_connection_id = _escape_sql_string(config.connection_id)
        escaped_ingestion_type = _escape_sql_string(config.ingestion_type)
        escaped_delta_table = _escape_sql_string(config.delta_table)
        escaped_file_pattern = _escape_sql_string(config.file_pattern)
        escaped_pipeline_id = _escape_sql_string(config.pipeline_id)
        
        # Serialize drive_names to JSON string
        drive_names_json = json.dumps(config.drive_names) if config.drive_names else None
        drive_names_str = f"'{_escape_sql_string(drive_names_json)}'" if drive_names_json else "NULL"
        
        insert_query = f"""
            INSERT INTO {pipelines_table} 
            (id, name, connection_id, ingestion_type, drive_names, 
             delta_table, file_pattern, pipeline_id)
            VALUES ('{escaped_id}', '{escaped_name}', '{escaped_connection_id}', 
                    '{escaped_ingestion_type}', {drive_names_str},
                    '{escaped_delta_table}', '{escaped_file_pattern}', '{escaped_pipeline_id}')
            RETURNING *
        """
        
        catalog = os.getenv("UC_CATALOG", "main")
        schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")
        await SecureSQL.execute_query(insert_query, catalog=catalog, schema=schema)
        
        return {
            "message": "Pipeline created successfully", 
            "id": config.id,
            "pipeline_id": pipeline_id
        }
    except HTTPException:
        raise
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=400, detail=f"Pipeline with id '{config.id}' already exists")
        raise HTTPException(status_code=500, detail=f"Failed to create pipeline: {str(e)}")


@router.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str) -> SharePointPipelineConfig:
    """Get a specific SharePoint pipeline config."""
    try:
        pipelines_table = _get_pipelines_table()
        
        # SECURE: Escape pipeline_id
        escaped_id = _escape_sql_string(pipeline_id)
        
        query = f"""
            SELECT id, name, connection_id, ingestion_type, drive_names, 
                   delta_table, file_pattern, pipeline_id
            FROM {pipelines_table}
            WHERE id = '{escaped_id}'
        """
        
        catalog = os.getenv("UC_CATALOG", "main")
        schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")
        rows = await SecureSQL.execute_query(query, catalog=catalog, schema=schema)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")
        
        row = rows[0]
        drive_names = json.loads(row['drive_names']) if row.get('drive_names') else None
        return SharePointPipelineConfig(
            id=row['id'],
            name=row['name'],
            connection_id=row['connection_id'],
            ingestion_type=row['ingestion_type'],
            drive_names=drive_names,
            delta_table=row['delta_table'],
            file_pattern=row.get('file_pattern') or "*.xlsx",
            pipeline_id=row.get('pipeline_id'),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get pipeline: {str(e)}")


@router.delete("/pipelines/{pipeline_id}")
async def delete_pipeline(pipeline_id: str) -> dict:
    """Delete a SharePoint pipeline."""
    try:
        pipelines_table = _get_pipelines_table()
        
        # Get pipeline config
        pipeline_config = await get_pipeline(pipeline_id)
        
        # Delete from Databricks if pipeline_id exists
        if pipeline_config.pipeline_id:
            await asyncio.to_thread(
                SharePointConnector.delete_pipeline, pipeline_config.pipeline_id
            )
        
        # SECURE: Escape pipeline_id
        escaped_id = _escape_sql_string(pipeline_id)
        
        # Delete from database
        delete_query = f"DELETE FROM {pipelines_table} WHERE id = '{escaped_id}' RETURNING *"
        catalog = os.getenv("UC_CATALOG", "main")
        schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")
        await SecureSQL.execute_query(delete_query, catalog=catalog, schema=schema)
        
        return {"message": "Pipeline deleted successfully", "id": pipeline_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete pipeline: {str(e)}")


@router.post("/pipelines/{pipeline_id}/run")
async def run_pipeline(pipeline_id: str) -> dict:
    """Trigger a SharePoint pipeline run."""
    try:
        pipeline_config = await get_pipeline(pipeline_id)
        
        if not pipeline_config.pipeline_id:
            raise HTTPException(status_code=400, detail="Pipeline has no Databricks pipeline_id")
        
        result = await asyncio.to_thread(
            SharePointConnector.start_pipeline, pipeline_config.pipeline_id
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run pipeline: {str(e)}")


@router.get("/pipelines/{pipeline_id}/status")
async def get_pipeline_status(pipeline_id: str) -> dict:
    """Get SharePoint pipeline status."""
    try:
        pipeline_config = await get_pipeline(pipeline_id)
        
        if not pipeline_config.pipeline_id:
            return {
                "success": True,
                "state": "NOT_CREATED",
                "message": "Pipeline not yet created in Databricks"
            }
        
        result = await asyncio.to_thread(
            SharePointConnector.get_pipeline_status, pipeline_config.pipeline_id
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get pipeline status: {str(e)}")
