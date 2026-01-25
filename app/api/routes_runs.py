# app/api/routes_runs.py
from fastapi import APIRouter, HTTPException
from app.core.models import SyncConfig
from app.core.pipeline import run_sync
from app.services.lakebase import Lakebase
import os

router = APIRouter()


def _get_configs_table():
    """Get the fully qualified configs table name."""
    catalog = os.getenv("LAKEBASE_CATALOG", "main")
    schema = os.getenv("LAKEBASE_SCHEMA", "vibe_coding")
    return f"{catalog}.{schema}.sync_configs"


@router.post("/run-once")
def run_once():
    # TODO: read from a configs table or request body; hardcode for now
    config = SyncConfig(
        id="supplier_a",
        catalog="main",
        schema_name="dastan_aitzhanov_supplier",
        file_name="supplier_a.xlsx",
    )
    result = run_sync(config)
    return result.dict()


@router.post("/run/{config_id}")
def run_by_config_id(config_id: str):
    """Run sync for a specific configuration by ID."""
    try:
        # Fetch config from database
        configs_table = _get_configs_table()
        query = f"""
            SELECT id, catalog, schema_name, file_name, documents_table, target_table
            FROM {configs_table}
            WHERE id = '{config_id}'
        """
        rows = Lakebase.query(query)
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Configuration '{config_id}' not found")
        
        # Build config from database row
        row = rows[0]
        config = SyncConfig(
            id=row[0],
            catalog=row[1],
            schema_name=row[2],
            file_name=row[3],
            documents_table=row[4],
            target_table=row[5],
        )
        
        # Run the sync
        result = run_sync(config)
        return result.dict()
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run sync: {str(e)}")
