# app/core/mcp_client.py
"""
MCP Client - Helper for calling Databricks MCP tools.
Provides a simplified interface that mirrors MCP tool functionality using Databricks SDK.
"""
from typing import Dict, Any, Optional, List
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def call_mcp_tool(server: str, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call an MCP tool using Databricks SDK.
    
    This function provides MCP-like tool calling interface while using
    Databricks SDK under the hood. It maps MCP tool patterns to SDK calls.
    
    Args:
        server: MCP server identifier (e.g., "project-0-fe-vibe-app-databricks")
        tool_name: Name of the tool to call (e.g., "execute_sql", "get_best_warehouse")
        arguments: Tool-specific arguments as a dictionary
        
    Returns:
        Tool result as a dictionary
        
    Raises:
        Exception: If tool call fails
        ValueError: If tool_name is not supported
    """
    # Validate server (for future multi-server support)
    if server != "project-0-fe-vibe-app-databricks":
        raise ValueError(f"Unknown MCP server: {server}")
    
    # Route to appropriate implementation based on tool name
    if tool_name == "get_best_warehouse":
        return _get_best_warehouse(**arguments)
    elif tool_name == "get_table_details":
        return _get_table_details(**arguments)
    elif tool_name == "execute_sql":
        return _execute_sql(**arguments)
    else:
        raise ValueError(f"Unknown MCP tool: {tool_name}")


def _get_workspace_client() -> WorkspaceClient:
    """Get or create Databricks Workspace Client."""
    return WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )


def _get_best_warehouse() -> Dict[str, Any]:
    """
    Get the ID of the best available SQL warehouse.
    
    Prioritizes running warehouses, then starting ones, preferring smaller sizes.
    
    Returns:
        {"result": warehouse_id} or {"result": None} if no warehouses available
    """
    try:
        w = _get_workspace_client()
        warehouses = list(w.warehouses.list())
        
        if not warehouses:
            return {"result": None}
        
        # Priority order: RUNNING > STARTING > STOPPED
        # Within same state, prefer smaller size (X-Small > Small > Medium, etc.)
        state_priority = {"RUNNING": 0, "STARTING": 1, "STOPPED": 2}
        size_priority = {"X_SMALL": 0, "SMALL": 1, "MEDIUM": 2, "LARGE": 3, "X_LARGE": 4, "XX_LARGE": 5}
        
        def warehouse_score(wh):
            state = wh.state.value if wh.state else "UNKNOWN"
            size = wh.cluster_size if wh.cluster_size else "MEDIUM"
            return (
                state_priority.get(state, 99),  # State priority (lower is better)
                size_priority.get(size, 99)     # Size priority (lower is better)
            )
        
        best_warehouse = min(warehouses, key=warehouse_score)
        return {"result": best_warehouse.id}
        
    except Exception as e:
        raise Exception(f"Failed to get best warehouse: {str(e)}")


def _get_table_details(
    catalog: str,
    schema: str,
    table_names: Optional[List[str]] = None,
    table_stat_level: str = "SIMPLE",
    warehouse_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get table schema and statistics for one or more tables.
    
    Args:
        catalog: Unity Catalog name
        schema: Schema name
        table_names: List of table names or GLOB patterns. If None, returns all tables.
        table_stat_level: "NONE", "SIMPLE", or "DETAILED"
        warehouse_id: Optional warehouse ID (auto-selected if not provided)
        
    Returns:
        {"tables": [...]} with table schemas and optional statistics
    """
    try:
        w = _get_workspace_client()
        
        # Get warehouse ID if not provided
        if warehouse_id is None:
            warehouse_result = _get_best_warehouse()
            warehouse_id = warehouse_result.get("result")
            if not warehouse_id:
                raise ValueError("No warehouse available")
        
        # List tables in schema
        full_schema = f"{catalog}.{schema}"
        all_tables = list(w.tables.list(catalog_name=catalog, schema_name=schema))
        
        # Filter by table names if provided
        if table_names:
            # Support GLOB patterns (simple implementation: * wildcard)
            import fnmatch
            filtered_tables = []
            for table in all_tables:
                for pattern in table_names:
                    if fnmatch.fnmatch(table.name, pattern):
                        filtered_tables.append(table)
                        break
            tables_to_process = filtered_tables
        else:
            tables_to_process = all_tables
        
        # Build result
        result_tables = []
        for table in tables_to_process:
            table_info = {
                "name": table.name,
                "table_type": table.table_type.value if table.table_type else "UNKNOWN",
                "columns": []
            }
            
            # Get column information
            if table.columns:
                for col in table.columns:
                    col_info = {
                        "name": col.name,
                        "type_name": col.type_name.value if col.type_name else str(col.type_text),
                        "nullable": col.nullable if col.nullable is not None else True,
                        "comment": col.comment
                    }
                    table_info["columns"].append(col_info)
            
            # Add statistics if requested
            if table_stat_level != "NONE":
                # Get basic stats via SQL
                try:
                    full_table = f"{catalog}.{schema}.{table.name}"
                    stats_query = f"DESCRIBE DETAIL {full_table}"
                    
                    statement = w.statement_execution.execute_statement(
                        warehouse_id=warehouse_id,
                        statement=stats_query,
                        wait_timeout="30s"
                    )
                    
                    if statement.status.state == StatementState.SUCCEEDED:
                        if statement.result and statement.result.data_array:
                            # Parse DESCRIBE DETAIL results
                            columns = [col.name for col in statement.manifest.schema.columns]
                            row = statement.result.data_array[0] if statement.result.data_array else []
                            detail = dict(zip(columns, row))
                            
                            table_info["row_count"] = detail.get("numRows")
                            table_info["size_bytes"] = detail.get("sizeInBytes")
                            table_info["last_updated"] = detail.get("lastModified")
                            
                except Exception as stats_error:
                    # Stats are optional, don't fail if we can't get them
                    print(f"Warning: Could not get stats for {table.name}: {stats_error}")
            
            result_tables.append(table_info)
        
        return {"tables": result_tables}
        
    except Exception as e:
        raise Exception(f"Failed to get table details: {str(e)}")


def _execute_sql(
    sql_query: str,
    warehouse_id: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    timeout: int = 50
) -> Dict[str, Any]:
    """
    Execute a SQL query on a Databricks SQL Warehouse.
    
    Args:
        sql_query: SQL query to execute
        warehouse_id: Optional warehouse ID (auto-selected if not provided)
        catalog: Optional catalog context for unqualified table names
        schema: Optional schema context for unqualified table names
        timeout: Timeout in seconds (default: 50, max: 50)
        
    Returns:
        {"result": [list of row dicts]}
    """
    try:
        w = _get_workspace_client()
        
        # Get warehouse ID if not provided
        if warehouse_id is None:
            warehouse_result = _get_best_warehouse()
            warehouse_id = warehouse_result.get("result")
            if not warehouse_id:
                raise ValueError("No warehouse available")
        
        # Validate timeout (Databricks limit: 5-50 seconds)
        if timeout < 5 or timeout > 50:
            timeout = min(max(timeout, 5), 50)  # Clamp to 5-50
        
        # Prepend USE statements if catalog/schema provided
        full_query = sql_query
        if catalog:
            full_query = f"USE CATALOG {catalog};\n{full_query}"
        if schema:
            full_query = f"USE SCHEMA {schema};\n{full_query}"
        
        # Execute statement
        statement = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=full_query,
            wait_timeout=f"{timeout}s"
        )
        
        # Wait for completion and get results
        if statement.status.state == StatementState.SUCCEEDED:
            # Parse results into list of dicts
            if statement.result and statement.result.data_array:
                columns = [col.name for col in statement.manifest.schema.columns]
                results = []
                for row in statement.result.data_array:
                    results.append(dict(zip(columns, row)))
                return {"result": results}
            return {"result": []}
        elif statement.status.state == StatementState.FAILED:
            error_msg = statement.status.error.message if statement.status.error else "Unknown error"
            raise Exception(f"Query failed: {error_msg}")
        else:
            raise Exception(f"Query in unexpected state: {statement.status.state}")
            
    except Exception as e:
        raise Exception(f"Failed to execute SQL: {str(e)}")
