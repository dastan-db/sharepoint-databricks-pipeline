# app/api/routes_sharepoint.py
from fastapi import APIRouter, HTTPException
from typing import List
import json
import os
from app.core.models import SharePointConnection, SharePointPipelineConfig
from app.services.unity_catalog import UnityCatalog
from app.services.sharepoint_connector import SharePointConnector

router = APIRouter()


# ============================================
# Connection Endpoints
# ============================================

@router.get("/connections")
def list_connections() -> List[SharePointConnection]:
    """List all SharePoint Lakeflow connector configurations."""
    try:
        # region agent log
        import time
        from databricks.sdk import WorkspaceClient
        import os
        with open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log', 'a') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"lakeflow-search","hypothesisId":"LAKEFLOW","location":"routes_sharepoint.py:21","message":"Searching for Lakeflow connectors","data":{},"timestamp":int(time.time()*1000)})+'\n')
        # endregion
        
        # Get all connections from Databricks
        # Use SHOW CONNECTIONS; SQL query via Unity Catalog
        query = "SHOW CONNECTIONS"
        results = UnityCatalog.query(query)
        
        # region agent log
        with open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log', 'a') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"sql-connections","hypothesisId":"SQL","location":"routes_sharepoint.py:30","message":"SHOW CONNECTIONS results","data":{"total":len(results),"sample":results[:5]},"timestamp":int(time.time()*1000)})+'\n')
        # endregion
        
        # Filter for SharePoint connections only
        sharepoint_connections = [
            conn for conn in results 
            if conn.get('type', '').upper() == 'SHAREPOINT'
        ]
        
        # Convert to SharePointConnection model
        connections = []
        for conn in sharepoint_connections:
            # Extract connection details
            conn_name = conn.get('name', '')
            conn_comment = conn.get('comment', '')
            connections.append(SharePointConnection(
                id=conn_name,
                name=conn_name,
                client_id="",  # Not exposed in SHOW CONNECTIONS
                client_secret="****",  # Not exposed
                tenant_id="",  # Would need to query connection details
                refresh_token="****",  # Not exposed
                site_id=conn_comment or "",  # Use comment as description
                connection_name=conn_name,
            ))
        
        return connections
    except Exception as e:
        # Handle query errors
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            return []
        raise HTTPException(status_code=500, detail=f"Failed to list connections: {str(e)}")


@router.post("/connections")
def create_connection(connection: SharePointConnection) -> dict:
    """Create a new SharePoint connection."""
    try:
        connections_table = _get_connections_table()
        
        # Create table if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {connections_table} (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                client_id VARCHAR NOT NULL,
                client_secret VARCHAR NOT NULL,
                tenant_id VARCHAR NOT NULL,
                refresh_token VARCHAR NOT NULL,
                site_id VARCHAR NOT NULL,
                connection_name VARCHAR NOT NULL
            )
        """
        Lakebase.query(create_table_query)
        
        # Generate connection name via service
        connection_name = SharePointConnector.create_connection(connection)
        if not connection.connection_name:
            connection.connection_name = connection_name
        
        # Insert the connection
        insert_query = f"""
            INSERT INTO {connections_table} 
            (id, name, client_id, client_secret, tenant_id, refresh_token, site_id, connection_name)
            VALUES ('{connection.id}', '{connection.name}', '{connection.client_id}', 
                    '{connection.client_secret}', '{connection.tenant_id}', '{connection.refresh_token}',
                    '{connection.site_id}', '{connection.connection_name}')
            RETURNING *
        """
        Lakebase.query(insert_query)
        
        return {"message": "Connection created successfully", "id": connection.id}
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=400, detail=f"Connection with id '{connection.id}' already exists")
        raise HTTPException(status_code=500, detail=f"Failed to create connection: {str(e)}")


@router.get("/connections/{connection_id}")
def get_connection(connection_id: str) -> SharePointConnection:
    """Get a specific SharePoint connection."""
    try:
        connections_table = _get_connections_table()
        query = f"""
            SELECT id, name, client_id, client_secret, tenant_id, refresh_token, site_id, connection_name
            FROM {connections_table}
            WHERE id = '{connection_id}'
        """
        rows = Lakebase.query(query)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")
        
        row = rows[0]
        return SharePointConnection(
            id=row[0],
            name=row[1],
            client_id=row[2],
            client_secret=row[3],
            tenant_id=row[4],
            refresh_token=row[5],
            site_id=row[6],
            connection_name=row[7],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get connection: {str(e)}")


@router.delete("/connections/{connection_id}")
def delete_connection(connection_id: str) -> dict:
    """Delete a SharePoint connection."""
    try:
        connections_table = _get_connections_table()
        
        # Check if connection exists
        check_query = f"SELECT id FROM {connections_table} WHERE id = '{connection_id}'"
        rows = Lakebase.query(check_query)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")
        
        # Delete the connection
        delete_query = f"DELETE FROM {connections_table} WHERE id = '{connection_id}' RETURNING *"
        Lakebase.query(delete_query)
        
        return {"message": "Connection deleted successfully", "id": connection_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete connection: {str(e)}")


@router.post("/connections/{connection_id}/test")
def test_connection(connection_id: str) -> dict:
    """Test a SharePoint connection."""
    try:
        # Get connection details
        connection = get_connection(connection_id)
        
        # Test the connection
        result = SharePointConnector.test_connection(connection)
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to test connection: {str(e)}")


# ============================================
# Pipeline Endpoints
# ============================================

@router.get("/pipelines")
def list_pipelines() -> List[SharePointPipelineConfig]:
    """List all SharePoint pipeline configs."""
    try:
        pipelines_table = _get_pipelines_table()
        query = f"""
            SELECT id, name, connection_id, ingestion_type, drive_names, 
                   delta_table, file_pattern, pipeline_id
            FROM {pipelines_table}
            ORDER BY name
        """
        rows = Lakebase.query(query)
        
        pipelines = []
        for row in rows:
            drive_names = json.loads(row[4]) if row[4] else None
            pipelines.append(SharePointPipelineConfig(
                id=row[0],
                name=row[1],
                connection_id=row[2],
                ingestion_type=row[3],
                drive_names=drive_names,
                delta_table=row[5],
                file_pattern=row[6] if row[6] else "*.xlsx",
                pipeline_id=row[7],
            ))
        return pipelines
    except Exception as e:
        # If table doesn't exist, return empty list
        if "does not exist" in str(e).lower() or "not found" in str(e).lower():
            return []
        raise HTTPException(status_code=500, detail=f"Failed to list pipelines: {str(e)}")


@router.post("/pipelines")
def create_pipeline(config: SharePointPipelineConfig) -> dict:
    """Create a new SharePoint ingestion pipeline targeting Lakebase."""
    try:
        pipelines_table = _get_pipelines_table()
        
        # Create table if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {pipelines_table} (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                connection_id VARCHAR NOT NULL,
                ingestion_type VARCHAR NOT NULL,
                drive_names VARCHAR,
                delta_table VARCHAR NOT NULL,
                file_pattern VARCHAR DEFAULT '*.xlsx',
                pipeline_id VARCHAR
            )
        """
        Lakebase.query(create_table_query)
        
        # Get connection details
        connection = get_connection(config.connection_id)
        
        # Create pipeline via service (targets Lakebase)
        pipeline_id = SharePointConnector.create_pipeline(config, connection)
        config.pipeline_id = pipeline_id
        
        # Serialize drive_names to JSON string
        drive_names_json = json.dumps(config.drive_names) if config.drive_names else None
        drive_names_str = f"'{drive_names_json}'" if drive_names_json else "NULL"
        
        # Insert the pipeline config
        insert_query = f"""
            INSERT INTO {pipelines_table} 
            (id, name, connection_id, ingestion_type, drive_names, 
             delta_table, file_pattern, pipeline_id)
            VALUES ('{config.id}', '{config.name}', '{config.connection_id}', 
                    '{config.ingestion_type}', {drive_names_str},
                    '{config.delta_table}', '{config.file_pattern}', '{config.pipeline_id}')
            RETURNING *
        """
        Lakebase.query(insert_query)
        
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
def get_pipeline(pipeline_id: str) -> SharePointPipelineConfig:
    """Get a specific SharePoint pipeline config."""
    try:
        pipelines_table = _get_pipelines_table()
        query = f"""
            SELECT id, name, connection_id, ingestion_type, drive_names, 
                   delta_table, file_pattern, pipeline_id
            FROM {pipelines_table}
            WHERE id = '{pipeline_id}'
        """
        rows = Lakebase.query(query)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")
        
        row = rows[0]
        drive_names = json.loads(row[4]) if row[4] else None
        return SharePointPipelineConfig(
            id=row[0],
            name=row[1],
            connection_id=row[2],
            ingestion_type=row[3],
            drive_names=drive_names,
            delta_table=row[5],
            file_pattern=row[6] if row[6] else "*.xlsx",
            pipeline_id=row[7],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get pipeline: {str(e)}")


@router.delete("/pipelines/{pipeline_id}")
def delete_pipeline(pipeline_id: str) -> dict:
    """Delete a SharePoint pipeline."""
    try:
        pipelines_table = _get_pipelines_table()
        
        # Get pipeline config
        pipeline_config = get_pipeline(pipeline_id)
        
        # Delete from Databricks if pipeline_id exists
        if pipeline_config.pipeline_id:
            SharePointConnector.delete_pipeline(pipeline_config.pipeline_id)
        
        # Delete from database
        delete_query = f"DELETE FROM {pipelines_table} WHERE id = '{pipeline_id}' RETURNING *"
        Lakebase.query(delete_query)
        
        return {"message": "Pipeline deleted successfully", "id": pipeline_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete pipeline: {str(e)}")


@router.post("/pipelines/{pipeline_id}/run")
def run_pipeline(pipeline_id: str) -> dict:
    """Trigger a SharePoint pipeline run."""
    try:
        # Get pipeline config
        pipeline_config = get_pipeline(pipeline_id)
        
        if not pipeline_config.pipeline_id:
            raise HTTPException(status_code=400, detail="Pipeline has no Databricks pipeline_id")
        
        # Start the pipeline
        result = SharePointConnector.start_pipeline(pipeline_config.pipeline_id)
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run pipeline: {str(e)}")


@router.get("/pipelines/{pipeline_id}/status")
def get_pipeline_status(pipeline_id: str) -> dict:
    """Get SharePoint pipeline status."""
    try:
        # Get pipeline config
        pipeline_config = get_pipeline(pipeline_id)
        
        if not pipeline_config.pipeline_id:
            return {
                "success": True,
                "state": "NOT_CREATED",
                "message": "Pipeline not yet created in Databricks"
            }
        
        # Get pipeline status
        result = SharePointConnector.get_pipeline_status(pipeline_config.pipeline_id)
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get pipeline status: {str(e)}")
