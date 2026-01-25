# app/api/routes_config.py
from fastapi import APIRouter, HTTPException
from typing import List
from app.core.models import SyncConfig
from app.services.lakebase import Lakebase
import os

router = APIRouter()

def _get_configs_table():
    """Get the fully qualified configs table name."""
    catalog = os.getenv("LAKEBASE_CATALOG", "main")
    schema = os.getenv("LAKEBASE_SCHEMA", "vibe_coding")
    return f"{catalog}.{schema}.sync_configs"


@router.get("")
def list_configs() -> List[SyncConfig]:
    """List all sync configurations."""
    try:
        try:
            configs_table = _get_configs_table()
            query = f"""
                SELECT id, catalog, schema_name, file_name, documents_table, target_table
                FROM {configs_table}
                ORDER BY id
            """
            rows = Lakebase.query(query)
            
            configs = []
            for row in rows:
                configs.append(SyncConfig(
                    id=row[0],
                    catalog=row[1],
                    schema_name=row[2],
                    file_name=row[3],
                    documents_table=row[4],
                    target_table=row[5],
                ))
            return configs
        except Exception as e:
            # If table doesn't exist, return empty list
            if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                return []
            raise HTTPException(status_code=500, detail=f"Failed to list configs: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("")
def create_config(config: SyncConfig) -> dict:
    """Create a new sync configuration."""
    try:
        configs_table = _get_configs_table()
        
        # Create table if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {configs_table} (
                id VARCHAR PRIMARY KEY,
                catalog VARCHAR NOT NULL,
                schema_name VARCHAR NOT NULL,
                file_name VARCHAR NOT NULL,
                documents_table VARCHAR NOT NULL,
                target_table VARCHAR NOT NULL
            )
        """
        Lakebase.query(create_table_query)
        
        # Insert the config
        insert_query = f"""
            INSERT INTO {configs_table} 
            (id, catalog, schema_name, file_name, documents_table, target_table)
            VALUES ('{config.id}', '{config.catalog}', '{config.schema_name}', 
                    '{config.file_name}', '{config.documents_table}', '{config.target_table}')
            RETURNING *
        """
        Lakebase.query(insert_query)
        
        return {"message": "Configuration created successfully", "id": config.id}
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=400, detail=f"Configuration with id '{config.id}' already exists")
        raise HTTPException(status_code=500, detail=f"Failed to create config: {str(e)}")


@router.get("/{config_id}")
def get_config(config_id: str) -> SyncConfig:
    """Get a specific sync configuration."""
    try:
        configs_table = _get_configs_table()
        query = f"""
            SELECT id, catalog, schema_name, file_name, documents_table, target_table
            FROM {configs_table}
            WHERE id = '{config_id}'
        """
        rows = Lakebase.query(query)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Configuration '{config_id}' not found")
        
        row = rows[0]
        return SyncConfig(
            id=row[0],
            catalog=row[1],
            schema_name=row[2],
            file_name=row[3],
            documents_table=row[4],
            target_table=row[5],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get config: {str(e)}")


@router.delete("/{config_id}")
def delete_config(config_id: str) -> dict:
    """Delete a sync configuration."""
    try:
        configs_table = _get_configs_table()
        
        # Check if config exists
        check_query = f"SELECT id FROM {configs_table} WHERE id = '{config_id}'"
        rows = Lakebase.query(check_query)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Configuration '{config_id}' not found")
        
        # Delete the config
        delete_query = f"DELETE FROM {configs_table} WHERE id = '{config_id}' RETURNING *"
        Lakebase.query(delete_query)
        
        return {"message": "Configuration deleted successfully", "id": config_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete config: {str(e)}")
