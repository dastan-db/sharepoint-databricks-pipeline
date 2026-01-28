"""
Test /configs endpoints (routes_config.py).
Tests CRUD operations for sync configurations stored in Lakebase.

NOTE: Tests skipped - Lakebase (PostgreSQL via Databricks) not used in this deployment.
"""
import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.skip(reason="Lakebase not used in this deployment")


def test_list_configs_empty(test_client: TestClient):
    """Test GET /configs returns empty list when no configs exist."""
    response = test_client.get("/configs")
    assert response.status_code == 200
    configs = response.json()
    assert isinstance(configs, list)


def test_create_config(test_client: TestClient, lakebase_catalog: str, lakebase_schema: str, cleanup_lakebase_tables):
    """Test POST /configs creates a new configuration."""
    # Register cleanup
    cleanup_lakebase_tables("sync_configs")
    
    config_data = {
        "id": "test_config_001",
        "catalog": lakebase_catalog,
        "schema_name": lakebase_schema,
        "file_name": "test_file.xlsx",
        "documents_table": "test_documents",
        "target_table": "test_target"
    }
    
    response = test_client.post("/configs", json=config_data)
    assert response.status_code == 200
    result = response.json()
    assert result["message"] == "Configuration created successfully"
    assert result["id"] == "test_config_001"


def test_create_duplicate_config(test_client: TestClient, lakebase_catalog: str, lakebase_schema: str, cleanup_lakebase_tables):
    """Test POST /configs fails when creating duplicate config."""
    cleanup_lakebase_tables("sync_configs")
    
    config_data = {
        "id": "test_config_duplicate",
        "catalog": lakebase_catalog,
        "schema_name": lakebase_schema,
        "file_name": "test_file.xlsx",
        "documents_table": "test_documents",
        "target_table": "test_target"
    }
    
    # Create first config
    response1 = test_client.post("/configs", json=config_data)
    assert response1.status_code == 200
    
    # Try to create duplicate
    response2 = test_client.post("/configs", json=config_data)
    assert response2.status_code == 400
    assert "already exists" in response2.json()["detail"].lower()


def test_get_config_by_id(test_client: TestClient, lakebase_catalog: str, lakebase_schema: str, cleanup_lakebase_tables):
    """Test GET /configs/{config_id} retrieves specific config."""
    cleanup_lakebase_tables("sync_configs")
    
    # Create config first
    config_data = {
        "id": "test_config_get",
        "catalog": lakebase_catalog,
        "schema_name": lakebase_schema,
        "file_name": "test_file.xlsx",
        "documents_table": "test_documents",
        "target_table": "test_target"
    }
    test_client.post("/configs", json=config_data)
    
    # Get config by ID
    response = test_client.get("/configs/test_config_get")
    assert response.status_code == 200
    config = response.json()
    assert config["id"] == "test_config_get"
    assert config["file_name"] == "test_file.xlsx"


def test_get_config_not_found(test_client: TestClient):
    """Test GET /configs/{config_id} returns 404 for non-existent config."""
    response = test_client.get("/configs/non_existent_config_xyz")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_delete_config(test_client: TestClient, lakebase_catalog: str, lakebase_schema: str, cleanup_lakebase_tables):
    """Test DELETE /configs/{config_id} removes config."""
    cleanup_lakebase_tables("sync_configs")
    
    # Create config first
    config_data = {
        "id": "test_config_delete",
        "catalog": lakebase_catalog,
        "schema_name": lakebase_schema,
        "file_name": "test_file.xlsx",
        "documents_table": "test_documents",
        "target_table": "test_target"
    }
    test_client.post("/configs", json=config_data)
    
    # Delete config
    response = test_client.delete("/configs/test_config_delete")
    assert response.status_code == 200
    result = response.json()
    assert result["message"] == "Configuration deleted successfully"
    assert result["id"] == "test_config_delete"
    
    # Verify it's gone
    response = test_client.get("/configs/test_config_delete")
    assert response.status_code == 404


def test_delete_config_not_found(test_client: TestClient):
    """Test DELETE /configs/{config_id} returns 404 for non-existent config."""
    response = test_client.delete("/configs/non_existent_config_xyz")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_list_configs_after_create(test_client: TestClient, lakebase_catalog: str, lakebase_schema: str, cleanup_lakebase_tables):
    """Test GET /configs returns configs after creating them."""
    cleanup_lakebase_tables("sync_configs")
    
    # Create multiple configs
    for i in range(3):
        config_data = {
            "id": f"test_config_list_{i}",
            "catalog": lakebase_catalog,
            "schema_name": lakebase_schema,
            "file_name": f"test_file_{i}.xlsx",
            "documents_table": "test_documents",
            "target_table": f"test_target_{i}"
        }
        test_client.post("/configs", json=config_data)
    
    # List configs
    response = test_client.get("/configs")
    assert response.status_code == 200
    configs = response.json()
    assert len(configs) >= 3
    
    # Verify our configs are in the list
    config_ids = [c["id"] for c in configs]
    assert "test_config_list_0" in config_ids
    assert "test_config_list_1" in config_ids
    assert "test_config_list_2" in config_ids
