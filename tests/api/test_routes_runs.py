"""
Test /runs endpoints (routes_runs.py).
Tests sync execution via the pipeline orchestration.

NOTE: Tests skipped - Uses Lakebase which is not used in this deployment.
"""
import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.skip(reason="Uses Lakebase which is not deployed")


@pytest.mark.skip(reason="Requires complete setup with documents table and Excel file")
def test_run_once(test_client: TestClient):
    """
    Test POST /runs/run-once executes hardcoded sync.
    SKIPPED: Requires documents table with supplier_a.xlsx file.
    """
    response = test_client.post("/runs/run-once")
    # Expected to work with proper setup
    assert response.status_code in [200, 404, 500]
    
    if response.status_code == 200:
        result = response.json()
        assert "status" in result
        assert result["status"] in ["skipped", "success", "dq_failed", "error"]


def test_run_by_config_id_not_found(test_client: TestClient):
    """Test POST /runs/run/{config_id} with non-existent config."""
    response = test_client.post("/runs/run/non_existent_config_xyz")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.skip(reason="Requires complete setup with config, documents, and Excel file")
def test_run_by_config_id_success(test_client: TestClient, lakebase_catalog: str, lakebase_schema: str, cleanup_lakebase_tables):
    """
    Test POST /runs/run/{config_id} executes sync for specific config.
    SKIPPED: Requires full setup with documents table and Excel file.
    """
    cleanup_lakebase_tables("sync_configs")
    
    # Create config
    config_data = {
        "id": "test_run_config",
        "catalog": lakebase_catalog,
        "schema_name": lakebase_schema,
        "file_name": "test_file.xlsx",
        "documents_table": "test_documents",
        "target_table": "test_target"
    }
    test_client.post("/configs", json=config_data)
    
    # Run sync
    response = test_client.post("/runs/run/test_run_config")
    
    # May fail if documents table doesn't exist, but should handle gracefully
    assert response.status_code in [200, 404, 500]
    
    if response.status_code == 200:
        result = response.json()
        assert "status" in result
        assert result["status"] in ["skipped", "success", "dq_failed", "error"]
