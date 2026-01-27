# app/api/routes_excel_streaming.py
"""
Excel Streaming Routes - Refactored for security.
Uses secure SQL practices for Lakebase (PostgreSQL) queries.
"""
from fastapi import APIRouter, HTTPException
from typing import List
import os
from app.core.models import ExcelStreamConfig
from app.services.lakebase import Lakebase
from app.services.excel_streaming import ExcelStreaming

router = APIRouter()


def _get_stream_configs_table():
    """Get the fully qualified excel_stream_configs table name."""
    schema = os.getenv("LAKEBASE_SCHEMA", "public")
    return f"{schema}.excel_stream_configs"


def _escape_sql_string(value: str) -> str:
    """Escape a string value for safe SQL interpolation in PostgreSQL."""
    if value is None:
        return "NULL"
    # Escape single quotes by doubling them
    return value.replace("'", "''")


def _create_configs_table_if_not_exists():
    """
    Create the excel_stream_configs table if it doesn't exist.
    This is for Lakebase (PostgreSQL), not Unity Catalog.
    """
    configs_table = _get_stream_configs_table()
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {configs_table} (
            id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            lakebase_table VARCHAR NOT NULL,
            file_name_pattern VARCHAR NOT NULL,
            destination_catalog VARCHAR NOT NULL,
            destination_schema VARCHAR NOT NULL,
            destination_table VARCHAR NOT NULL,
            checkpoint_location VARCHAR NOT NULL,
            trigger_interval VARCHAR DEFAULT '10 seconds',
            is_active BOOLEAN DEFAULT false
        )
    """
    Lakebase.query(create_table_query)


# ============================================
# Stream Config Management Endpoints
# ============================================

@router.get("/configs")
def list_stream_configs() -> List[ExcelStreamConfig]:
    """List all Excel streaming configurations."""
    try:
        configs_table = _get_stream_configs_table()
        query = f"""
            SELECT id, name, lakebase_table, file_name_pattern, 
                   destination_catalog, destination_schema, destination_table,
                   checkpoint_location, trigger_interval, is_active
            FROM {configs_table}
            ORDER BY name
        """
        rows = Lakebase.query(query)
        
        configs = []
        for row in rows:
            configs.append(ExcelStreamConfig(
                id=row[0],
                name=row[1],
                lakebase_table=row[2],
                file_name_pattern=row[3],
                destination_catalog=row[4],
                destination_schema=row[5],
                destination_table=row[6],
                checkpoint_location=row[7],
                trigger_interval=row[8],
                is_active=bool(row[9]) if row[9] is not None else False,
            ))
        return configs
    except Exception as e:
        if "does not exist" in str(e).lower() or "not found" in str(e).lower():
            return []
        raise HTTPException(status_code=500, detail=f"Failed to list stream configs: {str(e)}")


@router.post("/configs")
def create_stream_config(config: ExcelStreamConfig) -> dict:
    """Create a new Excel streaming configuration."""
    try:
        # Ensure table exists (PostgreSQL-specific)
        _create_configs_table_if_not_exists()
        
        configs_table = _get_stream_configs_table()
        
        # SECURE: Escape all values to prevent SQL injection
        escaped_id = _escape_sql_string(config.id)
        escaped_name = _escape_sql_string(config.name)
        escaped_lakebase_table = _escape_sql_string(config.lakebase_table)
        escaped_file_pattern = _escape_sql_string(config.file_name_pattern)
        escaped_dest_catalog = _escape_sql_string(config.destination_catalog)
        escaped_dest_schema = _escape_sql_string(config.destination_schema)
        escaped_dest_table = _escape_sql_string(config.destination_table)
        escaped_checkpoint = _escape_sql_string(config.checkpoint_location)
        escaped_interval = _escape_sql_string(config.trigger_interval)
        
        insert_query = f"""
            INSERT INTO {configs_table} 
            (id, name, lakebase_table, file_name_pattern, destination_catalog, 
             destination_schema, destination_table, checkpoint_location, trigger_interval, is_active)
            VALUES ('{escaped_id}', '{escaped_name}', '{escaped_lakebase_table}', 
                    '{escaped_file_pattern}', '{escaped_dest_catalog}', 
                    '{escaped_dest_schema}', '{escaped_dest_table}',
                    '{escaped_checkpoint}', '{escaped_interval}', {config.is_active})
            RETURNING *
        """
        Lakebase.query(insert_query)
        
        return {"message": "Stream configuration created successfully", "id": config.id}
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=400, detail=f"Configuration with id '{config.id}' already exists")
        raise HTTPException(status_code=500, detail=f"Failed to create stream config: {str(e)}")


@router.get("/configs/{config_id}")
def get_stream_config(config_id: str) -> ExcelStreamConfig:
    """Get a specific Excel streaming configuration."""
    try:
        configs_table = _get_stream_configs_table()
        
        # SECURE: Escape config_id
        escaped_id = _escape_sql_string(config_id)
        
        query = f"""
            SELECT id, name, lakebase_table, file_name_pattern, 
                   destination_catalog, destination_schema, destination_table,
                   checkpoint_location, trigger_interval, is_active
            FROM {configs_table}
            WHERE id = '{escaped_id}'
        """
        rows = Lakebase.query(query)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Stream configuration '{config_id}' not found")
        
        row = rows[0]
        return ExcelStreamConfig(
            id=row[0],
            name=row[1],
            lakebase_table=row[2],
            file_name_pattern=row[3],
            destination_catalog=row[4],
            destination_schema=row[5],
            destination_table=row[6],
            checkpoint_location=row[7],
            trigger_interval=row[8],
            is_active=bool(row[9]) if row[9] is not None else False,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stream config: {str(e)}")


@router.put("/configs/{config_id}")
def update_stream_config(config_id: str, config: ExcelStreamConfig) -> dict:
    """Update an Excel streaming configuration."""
    try:
        configs_table = _get_stream_configs_table()
        
        # SECURE: Escape all values
        escaped_config_id = _escape_sql_string(config_id)
        escaped_name = _escape_sql_string(config.name)
        escaped_lakebase_table = _escape_sql_string(config.lakebase_table)
        escaped_file_pattern = _escape_sql_string(config.file_name_pattern)
        escaped_dest_catalog = _escape_sql_string(config.destination_catalog)
        escaped_dest_schema = _escape_sql_string(config.destination_schema)
        escaped_dest_table = _escape_sql_string(config.destination_table)
        escaped_checkpoint = _escape_sql_string(config.checkpoint_location)
        escaped_interval = _escape_sql_string(config.trigger_interval)
        
        # Check if config exists
        check_query = f"SELECT id FROM {configs_table} WHERE id = '{escaped_config_id}'"
        rows = Lakebase.query(check_query)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Stream configuration '{config_id}' not found")
        
        # Update the config
        update_query = f"""
            UPDATE {configs_table}
            SET name = '{escaped_name}',
                lakebase_table = '{escaped_lakebase_table}',
                file_name_pattern = '{escaped_file_pattern}',
                destination_catalog = '{escaped_dest_catalog}',
                destination_schema = '{escaped_dest_schema}',
                destination_table = '{escaped_dest_table}',
                checkpoint_location = '{escaped_checkpoint}',
                trigger_interval = '{escaped_interval}',
                is_active = {config.is_active}
            WHERE id = '{escaped_config_id}'
            RETURNING *
        """
        Lakebase.query(update_query)
        
        return {"message": "Stream configuration updated successfully", "id": config_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update stream config: {str(e)}")


@router.delete("/configs/{config_id}")
def delete_stream_config(config_id: str) -> dict:
    """Delete an Excel streaming configuration."""
    try:
        configs_table = _get_stream_configs_table()
        
        # Check if config exists and stop stream if active
        config = get_stream_config(config_id)
        if config.is_active:
            ExcelStreaming.stop_stream(config_id)
        
        # SECURE: Escape config_id
        escaped_id = _escape_sql_string(config_id)
        
        # Delete the config
        delete_query = f"DELETE FROM {configs_table} WHERE id = '{escaped_id}' RETURNING *"
        Lakebase.query(delete_query)
        
        return {"message": "Stream configuration deleted successfully", "id": config_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete stream config: {str(e)}")


# ============================================
# Stream Control Endpoints
# ============================================

@router.post("/configs/{config_id}/start")
def start_stream(config_id: str) -> dict:
    """Start Excel streaming for a configuration."""
    try:
        # Get config
        config = get_stream_config(config_id)
        
        # Start the stream
        job_id = ExcelStreaming.start_stream(config)
        
        # SECURE: Escape config_id for UPDATE
        escaped_id = _escape_sql_string(config_id)
        
        # Update config to mark as active
        configs_table = _get_stream_configs_table()
        update_query = f"""
            UPDATE {configs_table}
            SET is_active = true
            WHERE id = '{escaped_id}'
            RETURNING *
        """
        Lakebase.query(update_query)
        
        return {
            "success": True,
            "message": f"Streaming started for {config.name}",
            "job_id": job_id
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start stream: {str(e)}")


@router.post("/configs/{config_id}/stop")
def stop_stream(config_id: str) -> dict:
    """Stop Excel streaming for a configuration."""
    try:
        # Get config
        config = get_stream_config(config_id)
        
        # Stop the stream
        result = ExcelStreaming.stop_stream(config_id)
        
        # SECURE: Escape config_id
        escaped_id = _escape_sql_string(config_id)
        
        # Update config to mark as inactive
        configs_table = _get_stream_configs_table()
        update_query = f"""
            UPDATE {configs_table}
            SET is_active = false
            WHERE id = '{escaped_id}'
            RETURNING *
        """
        Lakebase.query(update_query)
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop stream: {str(e)}")


@router.get("/configs/{config_id}/status")
def get_stream_status(config_id: str) -> dict:
    """Get status of Excel streaming for a configuration."""
    try:
        # Get config to verify it exists
        config = get_stream_config(config_id)
        
        # Get stream status
        status = ExcelStreaming.get_stream_status(config_id)
        
        return {
            **status,
            "config_name": config.name,
            "destination": f"{config.destination_catalog}.{config.destination_schema}.{config.destination_table}"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stream status: {str(e)}")
