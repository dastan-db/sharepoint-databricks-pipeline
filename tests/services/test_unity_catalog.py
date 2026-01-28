"""
Test UnityCatalog service (services/unity_catalog.py).
Tests Unity Catalog queries via MCP execute_sql.
"""
import pytest
from app.services.unity_catalog import UnityCatalog


def test_unity_catalog_is_singleton():
    """Test that UnityCatalog is a singleton."""
    from app.services.unity_catalog import _UnityCatalog
    instance1 = _UnityCatalog()
    instance2 = _UnityCatalog()
    assert instance1 is instance2


def test_unity_catalog_simple_query():
    """Test UnityCatalog.query() with simple SELECT."""
    result = UnityCatalog.query("SELECT 1 as test_value, 'hello' as test_string")
    
    assert isinstance(result, list)
    assert len(result) == 1
    # SQL results come back as strings from Databricks SQL
    assert str(result[0]["test_value"]) == "1"
    assert result[0]["test_string"] == "hello"


def test_unity_catalog_with_catalog_context(test_catalog: str):
    """Test UnityCatalog.query() with catalog context."""
    result = UnityCatalog.query(
        "SELECT current_catalog() as catalog_name",
        catalog=test_catalog
    )
    
    assert isinstance(result, list)
    assert len(result) == 1
    # Should use the specified catalog context


def test_unity_catalog_with_schema_context(test_catalog: str, test_schema: str):
    """Test UnityCatalog.query() with catalog and schema context."""
    result = UnityCatalog.query(
        "SELECT current_schema() as schema_name",
        catalog=test_catalog,
        schema=test_schema
    )
    
    assert isinstance(result, list)
    assert len(result) == 1


def test_unity_catalog_create_table(test_catalog: str, test_schema: str, cleanup_unity_tables):
    """Test UnityCatalog.query() creates table."""
    cleanup_unity_tables("test_uc_create")
    
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {test_catalog}.{test_schema}.test_uc_create (
            id STRING,
            value INT
        ) USING DELTA
    """
    result = UnityCatalog.query(create_query)
    
    # CREATE TABLE returns empty result
    assert isinstance(result, list)


def test_unity_catalog_insert_and_select(test_catalog: str, test_schema: str, cleanup_unity_tables):
    """Test UnityCatalog.query() insert and select operations."""
    cleanup_unity_tables("test_uc_insert")
    
    # Create table
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {test_catalog}.{test_schema}.test_uc_insert (
            id STRING,
            name STRING,
            value INT
        ) USING DELTA
    """
    UnityCatalog.query(create_query)
    
    # Insert data
    insert_query = f"""
        INSERT INTO {test_catalog}.{test_schema}.test_uc_insert
        VALUES ('id1', 'test_name', 100)
    """
    UnityCatalog.query(insert_query)
    
    # Select data
    select_query = f"""
        SELECT * FROM {test_catalog}.{test_schema}.test_uc_insert
        WHERE id = 'id1'
    """
    result = UnityCatalog.query(select_query)
    
    assert len(result) == 1
    assert result[0]["id"] == "id1"
    assert result[0]["name"] == "test_name"
    # SQL results come back as strings
    assert str(result[0]["value"]) == "100"


def test_unity_catalog_update(test_catalog: str, test_schema: str, cleanup_unity_tables):
    """Test UnityCatalog.query() update operations."""
    cleanup_unity_tables("test_uc_update")
    
    # Create and populate table
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {test_catalog}.{test_schema}.test_uc_update (
            id STRING,
            value INT
        ) USING DELTA
    """
    UnityCatalog.query(create_query)
    
    insert_query = f"""
        INSERT INTO {test_catalog}.{test_schema}.test_uc_update
        VALUES ('id1', 100)
    """
    UnityCatalog.query(insert_query)
    
    # Update
    update_query = f"""
        UPDATE {test_catalog}.{test_schema}.test_uc_update
        SET value = 200
        WHERE id = 'id1'
    """
    UnityCatalog.query(update_query)
    
    # Verify
    select_query = f"""
        SELECT * FROM {test_catalog}.{test_schema}.test_uc_update
        WHERE id = 'id1'
    """
    result = UnityCatalog.query(select_query)
    
    assert len(result) == 1
    # SQL results come back as strings
    assert str(result[0]["value"]) == "200"


def test_unity_catalog_delete(test_catalog: str, test_schema: str, cleanup_unity_tables):
    """Test UnityCatalog.query() delete operations."""
    cleanup_unity_tables("test_uc_delete")
    
    # Create and populate table
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {test_catalog}.{test_schema}.test_uc_delete (
            id STRING,
            name STRING
        ) USING DELTA
    """
    UnityCatalog.query(create_query)
    
    insert_query = f"""
        INSERT INTO {test_catalog}.{test_schema}.test_uc_delete
        VALUES ('id1', 'to_delete')
    """
    UnityCatalog.query(insert_query)
    
    # Delete
    delete_query = f"""
        DELETE FROM {test_catalog}.{test_schema}.test_uc_delete
        WHERE id = 'id1'
    """
    UnityCatalog.query(delete_query)
    
    # Verify deletion
    select_query = f"""
        SELECT * FROM {test_catalog}.{test_schema}.test_uc_delete
        WHERE id = 'id1'
    """
    result = UnityCatalog.query(select_query)
    
    assert len(result) == 0


def test_unity_catalog_with_timeout():
    """Test UnityCatalog.query() with custom timeout."""
    result = UnityCatalog.query(
        "SELECT 1 as value",
        timeout=10  # Short timeout for quick query
    )
    
    assert isinstance(result, list)
    assert len(result) == 1


def test_unity_catalog_error_handling():
    """Test UnityCatalog.query() handles invalid SQL."""
    with pytest.raises(Exception) as exc_info:
        UnityCatalog.query("INVALID SQL SYNTAX HERE")
    
    assert "failed" in str(exc_info.value).lower()


def test_unity_catalog_warehouse_selection():
    """Test UnityCatalog uses WarehouseManager for warehouse selection."""
    # This test verifies integration with WarehouseManager
    # If no warehouse_id is provided, it should auto-select via WarehouseManager
    result = UnityCatalog.query("SELECT 1 as value")
    
    assert isinstance(result, list)
    # If this succeeds, WarehouseManager successfully selected a warehouse
