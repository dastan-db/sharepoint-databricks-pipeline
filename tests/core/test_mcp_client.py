"""
Test MCP Client (core/mcp_client.py).
Tests MCP tool calling interface using Databricks SDK.
"""
import pytest
from app.core.mcp_client import call_mcp_tool


def test_call_mcp_tool_invalid_server():
    """Test call_mcp_tool() rejects invalid server name."""
    with pytest.raises(ValueError) as exc_info:
        call_mcp_tool(
            server="invalid-server-name",
            tool_name="get_best_warehouse",
            arguments={}
        )
    
    assert "unknown mcp server" in str(exc_info.value).lower()


def test_call_mcp_tool_invalid_tool():
    """Test call_mcp_tool() rejects invalid tool name."""
    with pytest.raises(ValueError) as exc_info:
        call_mcp_tool(
            server="project-0-fe-vibe-app-databricks",
            tool_name="invalid_tool_name",
            arguments={}
        )
    
    assert "unknown mcp tool" in str(exc_info.value).lower()


def test_get_best_warehouse():
    """Test get_best_warehouse tool returns warehouse ID."""
    result = call_mcp_tool(
        server="project-0-fe-vibe-app-databricks",
        tool_name="get_best_warehouse",
        arguments={}
    )
    
    assert isinstance(result, dict)
    assert "result" in result
    # May be None if no warehouses, but should be a valid response
    assert result["result"] is None or isinstance(result["result"], str)


def test_execute_sql_simple():
    """Test execute_sql tool with simple query."""
    result = call_mcp_tool(
        server="project-0-fe-vibe-app-databricks",
        tool_name="execute_sql",
        arguments={
            "sql_query": "SELECT 1 as value, 'test' as name"
        }
    )
    
    assert isinstance(result, dict)
    assert "result" in result
    assert isinstance(result["result"], list)
    assert len(result["result"]) == 1
    # SQL results come back as strings from Databricks SQL
    assert str(result["result"][0]["value"]) == "1"
    assert result["result"][0]["name"] == "test"


def test_execute_sql_with_catalog_context(test_catalog: str):
    """Test execute_sql with catalog context."""
    result = call_mcp_tool(
        server="project-0-fe-vibe-app-databricks",
        tool_name="execute_sql",
        arguments={
            "sql_query": "SELECT current_catalog() as catalog",
            "catalog": test_catalog
        }
    )
    
    assert isinstance(result, dict)
    assert "result" in result
    assert isinstance(result["result"], list)


def test_execute_sql_with_timeout():
    """Test execute_sql with custom timeout."""
    result = call_mcp_tool(
        server="project-0-fe-vibe-app-databricks",
        tool_name="execute_sql",
        arguments={
            "sql_query": "SELECT 1",
            "timeout": 10
        }
    )
    
    assert isinstance(result, dict)
    assert "result" in result


def test_execute_sql_with_warehouse_id():
    """Test execute_sql with explicit warehouse_id."""
    # Get a warehouse first
    warehouse_result = call_mcp_tool(
        server="project-0-fe-vibe-app-databricks",
        tool_name="get_best_warehouse",
        arguments={}
    )
    
    warehouse_id = warehouse_result["result"]
    if warehouse_id:
        result = call_mcp_tool(
            server="project-0-fe-vibe-app-databricks",
            tool_name="execute_sql",
            arguments={
                "sql_query": "SELECT 1 as value",
                "warehouse_id": warehouse_id
            }
        )
        
        assert isinstance(result, dict)
        assert "result" in result


def test_execute_sql_error_handling():
    """Test execute_sql handles invalid SQL."""
    with pytest.raises(Exception):
        call_mcp_tool(
            server="project-0-fe-vibe-app-databricks",
            tool_name="execute_sql",
            arguments={
                "sql_query": "INVALID SQL SYNTAX HERE"
            }
        )


def test_get_table_details_basic(test_catalog: str, test_schema: str):
    """Test get_table_details tool lists tables."""
    result = call_mcp_tool(
        server="project-0-fe-vibe-app-databricks",
        tool_name="get_table_details",
        arguments={
            "catalog": test_catalog,
            "schema": test_schema,
            "table_stat_level": "NONE"
        }
    )
    
    assert isinstance(result, dict)
    assert "tables" in result
    assert isinstance(result["tables"], list)


def test_get_table_details_with_pattern(test_catalog: str, test_schema: str):
    """Test get_table_details with table name pattern."""
    result = call_mcp_tool(
        server="project-0-fe-vibe-app-databricks",
        tool_name="get_table_details",
        arguments={
            "catalog": test_catalog,
            "schema": test_schema,
            "table_names": ["test_*"],
            "table_stat_level": "NONE"
        }
    )
    
    assert isinstance(result, dict)
    assert "tables" in result


def test_get_table_details_with_stats(test_catalog: str, test_schema: str):
    """Test get_table_details with statistics."""
    result = call_mcp_tool(
        server="project-0-fe-vibe-app-databricks",
        tool_name="get_table_details",
        arguments={
            "catalog": test_catalog,
            "schema": test_schema,
            "table_stat_level": "SIMPLE"
        }
    )
    
    assert isinstance(result, dict)
    assert "tables" in result
