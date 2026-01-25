# app/services/update_checker.py
from typing import Dict, Any
from app.core.models import SyncConfig
from app.services.lakebase import Lakebase


def should_process_file(config: SyncConfig) -> Dict[str, Any]:
    """
    Return a gating decision dict describing whether the Excel file
    has changed since the last table update.
    """
    docs_table_fq = f"{config.catalog}.{config.schema_name}.{config.documents_table}"
    target_table_fq = f"{config.catalog}.{config.schema_name}.{config.target_table}"

    result: Dict[str, Any] = {
        "catalog": config.catalog,
        "schema": config.schema_name,
        "file_name": config.file_name,
        "documents_table": docs_table_fq,
        "target_table": target_table_fq,
    }

    # 1) Look up file in documents table
    try:
        file_query = f"""
            SELECT file_metadata.last_modified_timestamp
            FROM {docs_table_fq}
            WHERE file_metadata.name = '{config.file_name}'
            LIMIT 1
        """
        file_rows = Lakebase.query(file_query)
        
        if not file_rows:
            result.update(
                {
                    "should_process": False,
                    "reason": f"File {config.file_name} not found in {docs_table_fq}",
                    "file_last_updated": None,
                    "table_last_updated": None,
                }
            )
            return result
        
        file_last_updated = file_rows[0][0]
    except Exception as e:
        result.update(
            {
                "should_process": False,
                "reason": f"Error reading documents table: {str(e)}",
                "file_last_updated": None,
                "table_last_updated": None,
            }
        )
        return result

    # 2) Try to read target table
    try:
        table_query = f"""
            SELECT last_modified
            FROM {target_table_fq}
            ORDER BY last_modified DESC
            LIMIT 1
        """
        table_rows = Lakebase.query(table_query)
        table_last_updated = table_rows[0][0] if table_rows else None
    except Exception:
        # Table doesn't exist yet – we should process
        result.update(
            {
                "should_process": True,
                "reason": "Target table does not exist; initial load required.",
                "file_last_updated": str(file_last_updated),
                "table_last_updated": None,
            }
        )
        return result

    should_process = (
        table_last_updated is None or file_last_updated > table_last_updated
    )

    result.update(
        {
            "should_process": bool(should_process),
            "reason": (
                "File has newer changes than table."
                if should_process
                else "No new updates detected; table is up to date."
            ),
            "file_last_updated": str(file_last_updated),
            "table_last_updated": str(table_last_updated)
            if table_last_updated
            else None,
        }
    )
    return result
