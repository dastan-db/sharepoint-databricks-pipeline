# app/api/routes_catalog.py
"""
Catalog Routes - Unity Catalog introspection and discovery.
Uses MCP get_table_details tool for schema discovery and statistics.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from pydantic import BaseModel
from app.core.mcp_client import call_mcp_tool

router = APIRouter()


class SchemaValidationRequest(BaseModel):
    table: str
    expected_columns: List[dict]


@router.get("/catalogs/{catalog}/schemas/{schema}/tables")
async def discover_tables(
    catalog: str,
    schema: str,
    pattern: Optional[str] = Query(None, description="Table name pattern (e.g., 'bronze_*', 'silver_*')"),
    include_stats: bool = Query(True, description="Include row counts and table statistics"),
    table_stat_level: str = Query("SIMPLE", description="Statistics level: NONE, SIMPLE, or DETAILED")
):
    """
    Discover tables in a Unity Catalog schema with optional pattern matching.
    
    ## Parameters
    - **catalog**: Unity Catalog name
    - **schema**: Schema name within the catalog
    - **pattern**: Optional GLOB pattern for table names (e.g., "bronze_*")
    - **include_stats**: Whether to include table statistics
    - **table_stat_level**: Level of statistics (NONE, SIMPLE, DETAILED)
    
    ## Returns
    List of tables with columns and optional statistics.
    
    ## Example
    ```
    GET /catalogs/main/schemas/default/tables?pattern=bronze_*&include_stats=true
    ```
    """
    try:
        # Determine stat level
        if not include_stats:
            stat_level = "NONE"
        else:
            stat_level = table_stat_level.upper()
            if stat_level not in ["NONE", "SIMPLE", "DETAILED"]:
                stat_level = "SIMPLE"
        
        # Build table names argument
        table_names = [pattern] if pattern else None
        
        # Call MCP tool
        result = call_mcp_tool(
            server="project-0-fe-vibe-app-databricks",
            tool_name="get_table_details",
            arguments={
                "catalog": catalog,
                "schema": schema,
                "table_names": table_names,
                "table_stat_level": stat_level
            }
        )
        
        # Transform result for API response
        tables = []
        for table in result.get("tables", []):
            table_info = {
                "name": table["name"],
                "full_name": f"{catalog}.{schema}.{table['name']}",
                "table_type": table.get("table_type"),
                "columns": [
                    {
                        "name": col["name"],
                        "type": col["type_name"],
                        "nullable": col.get("nullable", True),
                        "comment": col.get("comment")
                    }
                    for col in table.get("columns", [])
                ]
            }
            
            # Add statistics if requested
            if include_stats and stat_level != "NONE":
                table_info["statistics"] = {
                    "row_count": table.get("row_count"),
                    "size_bytes": table.get("size_bytes"),
                    "last_updated": table.get("last_updated")
                }
            
            tables.append(table_info)
        
        return {
            "catalog": catalog,
            "schema": schema,
            "pattern": pattern,
            "table_count": len(tables),
            "tables": tables
        }
        
    except NotImplementedError as e:
        # MCP client not yet implemented
        raise HTTPException(
            status_code=501,
            detail=f"MCP integration not yet configured: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to discover tables: {str(e)}"
        )


@router.get("/catalogs/{catalog}/schemas/{schema}/tables/{table}/schema")
async def get_table_schema(
    catalog: str,
    schema: str,
    table: str,
    include_stats: bool = Query(False, description="Include column-level statistics")
):
    """
    Get detailed schema for a specific table.
    
    ## Parameters
    - **catalog**: Unity Catalog name
    - **schema**: Schema name
    - **table**: Table name
    - **include_stats**: Include column-level statistics (DETAILED mode)
    
    ## Returns
    Table schema with columns and optional statistics.
    """
    try:
        stat_level = "DETAILED" if include_stats else "NONE"
        
        result = call_mcp_tool(
            server="project-0-fe-vibe-app-databricks",
            tool_name="get_table_details",
            arguments={
                "catalog": catalog,
                "schema": schema,
                "table_names": [table],
                "table_stat_level": stat_level
            }
        )
        
        tables = result.get("tables", [])
        if not tables:
            raise HTTPException(
                status_code=404,
                detail=f"Table {catalog}.{schema}.{table} not found"
            )
        
        table_info = tables[0]
        
        return {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "full_name": f"{catalog}.{schema}.{table}",
            "table_type": table_info.get("table_type"),
            "columns": table_info.get("columns", []),
            "statistics": {
                "row_count": table_info.get("row_count"),
                "size_bytes": table_info.get("size_bytes")
            } if include_stats else None
        }
        
    except NotImplementedError as e:
        raise HTTPException(
            status_code=501,
            detail=f"MCP integration not yet configured: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get table schema: {str(e)}"
        )


@router.post("/catalogs/{catalog}/schemas/{schema}/validate-schema")
async def validate_table_schema(
    catalog: str,
    schema: str,
    request: SchemaValidationRequest
):
    """
    Validate that a table has expected columns with correct types.
    
    ## Request Body
    ```json
    {
        "table": "my_table",
        "expected_columns": [
            {"name": "id", "type": "STRING"},
            {"name": "value", "type": "INT"}
        ]
    }
    ```
    
    ## Returns
    Validation result with missing columns and type mismatches.
    """
    table = request.table
    expected_columns = request.expected_columns
    try:
        # Get actual schema
        result = call_mcp_tool(
            server="project-0-fe-vibe-app-databricks",
            tool_name="get_table_details",
            arguments={
                "catalog": catalog,
                "schema": schema,
                "table_names": [table],
                "table_stat_level": "NONE"
            }
        )
        
        tables = result.get("tables", [])
        if not tables:
            raise HTTPException(
                status_code=404,
                detail=f"Table {catalog}.{schema}.{table} not found"
            )
        
        # Build actual columns map
        actual_columns = {
            col["name"]: col["type_name"]
            for col in tables[0].get("columns", [])
        }
        
        # Build expected columns map
        expected = {col["name"]: col["type"] for col in expected_columns}
        
        # Validate
        missing = set(expected.keys()) - set(actual_columns.keys())
        extra = set(actual_columns.keys()) - set(expected.keys())
        type_mismatches = {
            col: {"expected": expected[col], "actual": actual_columns[col]}
            for col in expected
            if col in actual_columns and actual_columns[col] != expected[col]
        }
        
        is_valid = not missing and not type_mismatches
        
        return {
            "valid": is_valid,
            "table": f"{catalog}.{schema}.{table}",
            "missing_columns": list(missing),
            "extra_columns": list(extra),
            "type_mismatches": type_mismatches,
            "message": "Schema is valid" if is_valid else "Schema validation failed"
        }
        
    except NotImplementedError as e:
        raise HTTPException(
            status_code=501,
            detail=f"MCP integration not yet configured: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to validate schema: {str(e)}"
        )
