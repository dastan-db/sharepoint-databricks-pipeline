"""
Test /api/catalog endpoints (routes_catalog.py).
Tests Unity Catalog introspection and discovery via MCP tools.
"""
import pytest
from fastapi.testclient import TestClient


def test_discover_tables(test_client: TestClient, test_catalog: str, test_schema: str):
    """Test GET /api/catalog/catalogs/{catalog}/schemas/{schema}/tables."""
    response = test_client.get(f"/api/catalog/catalogs/{test_catalog}/schemas/{test_schema}/tables")
    assert response.status_code == 200
    result = response.json()
    assert "catalog" in result
    assert "schema" in result
    assert "tables" in result
    assert "table_count" in result
    assert isinstance(result["tables"], list)


def test_discover_tables_with_pattern(test_client: TestClient, test_catalog: str, test_schema: str):
    """Test table discovery with pattern matching."""
    response = test_client.get(
        f"/api/catalog/catalogs/{test_catalog}/schemas/{test_schema}/tables",
        params={"pattern": "test_*", "include_stats": False}
    )
    assert response.status_code == 200
    result = response.json()
    assert result["pattern"] == "test_*"
    assert isinstance(result["tables"], list)


def test_discover_tables_with_stats(test_client: TestClient, test_catalog: str, test_schema: str):
    """Test table discovery with statistics."""
    response = test_client.get(
        f"/api/catalog/catalogs/{test_catalog}/schemas/{test_schema}/tables",
        params={"include_stats": True, "table_stat_level": "SIMPLE"}
    )
    assert response.status_code == 200
    result = response.json()
    assert isinstance(result["tables"], list)


def test_get_table_schema_not_found(test_client: TestClient, test_catalog: str, test_schema: str):
    """Test GET table schema for non-existent table."""
    response = test_client.get(
        f"/api/catalog/catalogs/{test_catalog}/schemas/{test_schema}/tables/non_existent_table_xyz/schema"
    )
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_validate_schema_missing_table(test_client: TestClient, test_catalog: str, test_schema: str):
    """Test POST validate-schema with non-existent table."""
    validation_request = {
        "table": "non_existent_table_xyz",
        "expected_columns": [
            {"name": "id", "type": "STRING"},
            {"name": "value", "type": "INT"}
        ]
    }
    
    response = test_client.post(
        f"/api/catalog/catalogs/{test_catalog}/schemas/{test_schema}/validate-schema",
        json=validation_request
    )
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.skip(reason="Requires existing table with known schema")
def test_get_table_schema_success(test_client: TestClient, test_catalog: str, test_schema: str):
    """
    Test GET table schema for existing table.
    SKIPPED: Requires pre-existing table in test schema.
    """
    response = test_client.get(
        f"/api/catalog/catalogs/{test_catalog}/schemas/{test_schema}/tables/existing_table/schema"
    )
    assert response.status_code == 200
    result = response.json()
    assert "columns" in result
    assert "full_name" in result


@pytest.mark.skip(reason="Requires existing table with known schema")
def test_validate_schema_success(test_client: TestClient, test_catalog: str, test_schema: str):
    """
    Test schema validation for existing table.
    SKIPPED: Requires pre-existing table with known schema.
    """
    validation_request = {
        "table": "existing_table",
        "expected_columns": [
            {"name": "id", "type": "STRING"}
        ]
    }
    
    response = test_client.post(
        f"/api/catalog/catalogs/{test_catalog}/schemas/{test_schema}/validate-schema",
        json=validation_request
    )
    assert response.status_code == 200
    result = response.json()
    assert "valid" in result
