# app/api/routes_excel_streaming.py
from fastapi import APIRouter, HTTPException
from typing import List
import os
from app.core.models import ExcelStreamConfig
from app.services.lakebase import Lakebase
from app.services.excel_streaming import ExcelStreaming

router = APIRouter()


def _get_stream_configs_table():
    """Get the fully qualified excel_stream_configs table name."""
    catalog = os.getenv("LAKEBASE_CATALOG", "main")
    schema = os.getenv("LAKEBASE_SCHEMA", "vibe_coding")
    return f"{catalog}.{schema}.excel_stream_configs"


# ============================================
# Stream Config Management Endpoints
# ============================================

@router.get("/configs")
def list_stream_configs() -> List[ExcelStreamConfig]:
    """List all Excel streaming configurations."""
    # region agent log
    import json, time
    with open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log', 'a') as f:
        f.write(json.dumps({'location':'routes_excel_streaming.py:22','message':'list_stream_configs called','data':{},'timestamp':int(time.time()*1000),'sessionId':'debug-session','hypothesisId':'H3,H5'}) + '\n')
    # endregion
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
        # If table doesn't exist, return empty list
        if "does not exist" in str(e).lower() or "not found" in str(e).lower():
            return []
        raise HTTPException(status_code=500, detail=f"Failed to list stream configs: {str(e)}")


@router.post("/configs")
def create_stream_config(config: ExcelStreamConfig) -> dict:
    """Create a new Excel streaming configuration."""
    try:
        configs_table = _get_stream_configs_table()
        
        # Create table if it doesn't exist
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
        
        # Insert the config
        insert_query = f"""
            INSERT INTO {configs_table} 
            (id, name, lakebase_table, file_name_pattern, destination_catalog, 
             destination_schema, destination_table, checkpoint_location, trigger_interval, is_active)
            VALUES ('{config.id}', '{config.name}', '{config.lakebase_table}', 
                    '{config.file_name_pattern}', '{config.destination_catalog}', 
                    '{config.destination_schema}', '{config.destination_table}',
                    '{config.checkpoint_location}', '{config.trigger_interval}', {config.is_active})
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
        query = f"""
            SELECT id, name, lakebase_table, file_name_pattern, 
                   destination_catalog, destination_schema, destination_table,
                   checkpoint_location, trigger_interval, is_active
            FROM {configs_table}
            WHERE id = '{config_id}'
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
        
        # Check if config exists
        check_query = f"SELECT id FROM {configs_table} WHERE id = '{config_id}'"
        rows = Lakebase.query(check_query)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Stream configuration '{config_id}' not found")
        
        # Update the config
        update_query = f"""
            UPDATE {configs_table}
            SET name = '{config.name}',
                lakebase_table = '{config.lakebase_table}',
                file_name_pattern = '{config.file_name_pattern}',
                destination_catalog = '{config.destination_catalog}',
                destination_schema = '{config.destination_schema}',
                destination_table = '{config.destination_table}',
                checkpoint_location = '{config.checkpoint_location}',
                trigger_interval = '{config.trigger_interval}',
                is_active = {config.is_active}
            WHERE id = '{config_id}'
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
        
        # Delete the config
        delete_query = f"DELETE FROM {configs_table} WHERE id = '{config_id}' RETURNING *"
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
        
        # Update config to mark as active
        configs_table = _get_stream_configs_table()
        update_query = f"""
            UPDATE {configs_table}
            SET is_active = true
            WHERE id = '{config_id}'
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
        
        # Update config to mark as inactive
        configs_table = _get_stream_configs_table()
        update_query = f"""
            UPDATE {configs_table}
            SET is_active = false
            WHERE id = '{config_id}'
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
