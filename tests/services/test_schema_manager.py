"""
Test SchemaManager service (services/schema_manager.py).
Tests database schema initialization on application startup.
"""
import pytest
import pytest_asyncio
from app.services.schema_manager import SchemaManager


def test_schema_manager_is_singleton():
    """Test that SchemaManager is a singleton."""
    from app.services.schema_manager import _SchemaManagerService
    instance1 = _SchemaManagerService()
    instance2 = _SchemaManagerService()
    assert instance1 is instance2


# Legacy tests removed - methods no longer exist
# initialize_sharepoint_tables() and initialize_lakebase_tables() have been deleted


@pytest.mark.asyncio
async def test_schema_manager_ensure_catalog_and_schema_exist(test_catalog: str, test_schema: str):
    """Test _ensure_catalog_and_schema_exist() creates catalog/schema."""
    # This is a private method, but we can test it via the manager
    await SchemaManager._ensure_catalog_and_schema_exist(test_catalog, test_schema)
    
    # If no exception raised, catalog and schema were created or already exist
    # Success is indicated by no exception


@pytest.mark.skip(reason="Requires specific table schema setup")
@pytest.mark.asyncio
async def test_schema_manager_ensure_table_exists(test_catalog: str, test_schema: str):
    """
    Test _ensure_table_exists() creates table if it doesn't exist.
    SKIPPED: Requires ColumnInfo objects and specific setup.
    """
    from databricks.sdk.service.catalog import ColumnInfo, ColumnTypeName
    
    columns = [
        ColumnInfo(name="id", type_name=ColumnTypeName.STRING, nullable=False),
        ColumnInfo(name="value", type_name=ColumnTypeName.INT, nullable=True)
    ]
    
    await SchemaManager._ensure_table_exists(
        catalog=test_catalog,
        schema=test_schema,
        table_name="test_schema_mgr_table",
        columns=columns,
        comment="Test table"
    )
    
    # Table should now exist
