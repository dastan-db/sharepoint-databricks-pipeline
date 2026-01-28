"""
End-to-end integration tests for sync workflow.
Tests the complete flow: Create config → Run sync → Verify data
"""
import pytest
from fastapi.testclient import TestClient


@pytest.mark.skip(reason="Requires full environment setup with documents table and Excel file")
def test_complete_sync_workflow(
    test_client: TestClient,
    lakebase_catalog: str,
    lakebase_schema: str,
    sample_excel_file: bytes,
    cleanup_lakebase_tables
):
    """
    Test complete sync workflow from config creation to data verification.
    
    Flow:
    1. Create sync config via POST /configs
    2. (Assume documents table has Excel file)
    3. Run sync via POST /runs/run/{config_id}
    4. Verify data was processed
    5. Verify data quality checks passed
    6. Clean up
    
    SKIPPED: Requires documents table with Excel file.
    """
    cleanup_lakebase_tables("sync_configs")
    cleanup_lakebase_tables("test_e2e_target")
    
    # Step 1: Create config
    config_data = {
        "id": "test_e2e_config",
        "catalog": lakebase_catalog,
        "schema_name": lakebase_schema,
        "file_name": "test_supplier.xlsx",
        "documents_table": "test_documents",
        "target_table": "test_e2e_target"
    }
    
    create_response = test_client.post("/configs", json=config_data)
    assert create_response.status_code == 200
    
    # Step 2: Run sync
    run_response = test_client.post("/runs/run/test_e2e_config")
    assert run_response.status_code == 200
    
    result = run_response.json()
    assert "status" in result
    assert result["status"] in ["success", "skipped", "dq_failed", "error"]
    
    # Step 3: Verify results based on status
    if result["status"] == "success":
        assert result["parse"]["status"] == "success"
        assert result["parse"]["rows_processed"] > 0
        assert result["dq"]["checks_passed"] is True
    
    # Step 4: Clean up - delete config
    delete_response = test_client.delete("/configs/test_e2e_config")
    assert delete_response.status_code == 200


@pytest.mark.skip(reason="Requires full environment setup")
def test_sync_workflow_with_updates(
    test_client: TestClient,
    lakebase_catalog: str,
    lakebase_schema: str,
    cleanup_lakebase_tables
):
    """
    Test sync workflow detects and processes file updates.
    
    Flow:
    1. Run initial sync
    2. Verify status is 'success'
    3. Run sync again without file change
    4. Verify status is 'skipped'
    5. (Simulate file update)
    6. Run sync again
    7. Verify status is 'success'
    
    SKIPPED: Requires documents table with updateable Excel file.
    """
    pass


@pytest.mark.skip(reason="Requires full environment setup")
def test_sync_workflow_with_data_quality_failure(
    test_client: TestClient,
    lakebase_catalog: str,
    lakebase_schema: str,
    cleanup_lakebase_tables
):
    """
    Test sync workflow handles data quality failures.
    
    Flow:
    1. Create config
    2. Run sync with bad data
    3. Verify status is 'dq_failed'
    4. Verify DQ checks indicate failures
    
    SKIPPED: Requires documents table with Excel file containing bad data.
    """
    pass


@pytest.mark.skip(reason="Requires full environment setup")
def test_multiple_configs_sync(
    test_client: TestClient,
    lakebase_catalog: str,
    lakebase_schema: str,
    cleanup_lakebase_tables
):
    """
    Test managing and running multiple sync configurations.
    
    Flow:
    1. Create multiple configs
    2. List configs via GET /configs
    3. Run each config individually
    4. Verify all syncs complete
    5. Delete all configs
    
    SKIPPED: Requires documents table with multiple Excel files.
    """
    cleanup_lakebase_tables("sync_configs")
    
    # Create 3 configs
    config_ids = []
    for i in range(3):
        config_data = {
            "id": f"test_multi_config_{i}",
            "catalog": lakebase_catalog,
            "schema_name": lakebase_schema,
            "file_name": f"test_file_{i}.xlsx",
            "documents_table": "test_documents",
            "target_table": f"test_target_{i}"
        }
        
        response = test_client.post("/configs", json=config_data)
        assert response.status_code == 200
        config_ids.append(config_data["id"])
    
    # List configs
    list_response = test_client.get("/configs")
    assert list_response.status_code == 200
    configs = list_response.json()
    assert len(configs) >= 3
    
    # Run each sync (would fail without documents, but tests the flow)
    for config_id in config_ids:
        run_response = test_client.post(f"/runs/run/{config_id}")
        assert run_response.status_code in [200, 404, 500]
    
    # Clean up
    for config_id in config_ids:
        test_client.delete(f"/configs/{config_id}")


@pytest.mark.skip(reason="Uses Lakebase which is not deployed")
def test_config_crud_integration(
    test_client: TestClient,
    lakebase_catalog: str,
    lakebase_schema: str,
    cleanup_lakebase_tables
):
    """Test complete CRUD cycle for sync configurations."""
    cleanup_lakebase_tables("sync_configs")
    
    # Create
    config_data = {
        "id": "test_crud_config",
        "catalog": lakebase_catalog,
        "schema_name": lakebase_schema,
        "file_name": "test_crud.xlsx",
        "documents_table": "test_docs",
        "target_table": "test_crud_target"
    }
    
    create_response = test_client.post("/configs", json=config_data)
    assert create_response.status_code == 200
    
    # Read
    get_response = test_client.get("/configs/test_crud_config")
    assert get_response.status_code == 200
    config = get_response.json()
    assert config["id"] == "test_crud_config"
    assert config["file_name"] == "test_crud.xlsx"
    
    # List
    list_response = test_client.get("/configs")
    assert list_response.status_code == 200
    configs = list_response.json()
    config_ids = [c["id"] for c in configs]
    assert "test_crud_config" in config_ids
    
    # Delete
    delete_response = test_client.delete("/configs/test_crud_config")
    assert delete_response.status_code == 200
    
    # Verify deleted
    verify_response = test_client.get("/configs/test_crud_config")
    assert verify_response.status_code == 404


@pytest.mark.skip(reason="Requires error condition setup")
def test_sync_error_handling(
    test_client: TestClient,
    lakebase_catalog: str,
    lakebase_schema: str,
    cleanup_lakebase_tables
):
    """
    Test sync workflow handles errors gracefully.
    
    Tests:
    - Missing documents table
    - Malformed Excel file
    - Database connection errors
    - Target table creation errors
    
    SKIPPED: Requires specific error condition setup.
    """
    pass
