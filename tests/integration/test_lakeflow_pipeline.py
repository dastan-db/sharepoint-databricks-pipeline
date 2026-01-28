"""
End-to-end integration tests for Lakeflow pipeline workflow.
Tests: Create connection → Create job → Configure sync → Run
"""
import pytest
from fastapi.testclient import TestClient


@pytest.mark.skip(reason="Requires valid SharePoint connection in Unity Catalog")
def test_complete_lakeflow_workflow(
    test_client: TestClient,
    test_catalog: str,
    test_schema: str,
    workspace_client,
    cleanup_lakeflow_jobs,
    cleanup_unity_tables
):
    """
    Test complete Lakeflow pipeline workflow.
    
    Flow:
    1. (Assume SharePoint connection exists)
    2. Create Lakeflow job via POST /api/lakeflow/jobs
    3. Wait for pipeline to complete
    4. Check job status via GET /api/lakeflow/jobs/{id}/status
    5. Query documents via GET /api/lakeflow/jobs/{id}/documents
    6. Configure sync via POST /api/lakeflow/jobs/{id}/configure-sync
    7. Run sync via POST /api/lakeflow/jobs/{id}/run-sync
    8. Verify target table created
    9. Clean up resources
    
    SKIPPED: Requires valid SharePoint connection.
    """
    cleanup_unity_tables("lakeflow_jobs")
    
    # Step 1: Create Lakeflow job
    job_config = {
        "connection_id": "test_lakeflow_e2e",
        "connection_name": "existing-sharepoint-connection",
        "source_schema": "00000000-0000-0000-0000-000000000000",
        "destination_catalog": test_catalog,
        "destination_schema": test_schema
    }
    
    create_response = test_client.post("/api/lakeflow/jobs", json=job_config)
    assert create_response.status_code == 200
    
    job_result = create_response.json()
    connection_id = job_result["connection_id"]
    job_id = job_result["job_id"]
    pipeline_id = job_result["document_pipeline_id"]
    
    # Register for cleanup
    cleanup_lakeflow_jobs["job"](job_id)
    cleanup_lakeflow_jobs["pipeline"](pipeline_id)
    
    # Step 2: Check job status
    status_response = test_client.get(f"/api/lakeflow/jobs/{connection_id}/status")
    assert status_response.status_code == 200
    
    # Step 3: Query documents (may be empty initially)
    docs_response = test_client.get(f"/api/lakeflow/jobs/{connection_id}/documents")
    assert docs_response.status_code == 200
    
    # Step 4: Configure sync for an Excel file
    sync_config = {
        "file_path": "test_file.xlsx",
        "table_name": "test_lakeflow_target",
        "header_row": 0,
        "selected_columns": ["col1", "col2"]
    }
    
    configure_response = test_client.post(
        f"/api/lakeflow/jobs/{connection_id}/configure-sync",
        json=sync_config
    )
    assert configure_response.status_code == 200
    
    # Step 5: Run sync
    run_response = test_client.post(f"/api/lakeflow/jobs/{connection_id}/run-sync")
    assert run_response.status_code == 200
    
    # Step 6: Clean up - delete job
    delete_response = test_client.delete(f"/api/lakeflow/jobs/{connection_id}")
    assert delete_response.status_code == 200


@pytest.mark.skip(reason="Requires SharePoint connection")
def test_lakeflow_job_listing(test_client: TestClient, test_catalog: str, test_schema: str):
    """
    Test listing Lakeflow jobs.
    
    Flow:
    1. List jobs (may be empty)
    2. Create a job
    3. List jobs again
    4. Verify new job appears
    
    SKIPPED: Requires SharePoint connection setup.
    """
    # List initial jobs
    list_response = test_client.get("/api/lakeflow/jobs")
    assert list_response.status_code == 200
    initial_jobs = list_response.json()
    initial_count = len(initial_jobs)
    
    # Would create job here...
    # Then verify it appears in the list


@pytest.mark.skip(reason="Requires SharePoint connection")
def test_lakeflow_sync_disable_reenable(
    test_client: TestClient,
    test_catalog: str,
    test_schema: str
):
    """
    Test disabling and re-enabling sync for a Lakeflow job.
    
    Flow:
    1. Create job and configure sync
    2. Verify sync is enabled
    3. Disable sync via DELETE /api/lakeflow/jobs/{id}/disable-sync
    4. Verify sync is disabled
    5. Re-configure sync
    6. Verify sync is enabled again
    
    SKIPPED: Requires SharePoint connection.
    """
    pass


def test_lakeflow_job_not_found_scenarios(test_client: TestClient):
    """Test Lakeflow endpoints handle non-existent jobs."""
    fake_connection_id = "non_existent_connection_xyz"
    
    # Status endpoint
    status_response = test_client.get(f"/api/lakeflow/jobs/{fake_connection_id}/status")
    assert status_response.status_code == 404
    
    # Documents endpoint
    docs_response = test_client.get(f"/api/lakeflow/jobs/{fake_connection_id}/documents")
    assert docs_response.status_code == 404
    
    # Configure sync endpoint
    sync_config = {
        "file_path": "test.xlsx",
        "table_name": "test_table",
        "header_row": 0
    }
    configure_response = test_client.post(
        f"/api/lakeflow/jobs/{fake_connection_id}/configure-sync",
        json=sync_config
    )
    assert configure_response.status_code == 404
    
    # Run sync endpoint
    run_response = test_client.post(f"/api/lakeflow/jobs/{fake_connection_id}/run-sync")
    assert run_response.status_code == 404


@pytest.mark.skip(reason="Requires SharePoint connection")
def test_excel_preview_and_parse_workflow(
    test_client: TestClient,
    test_catalog: str,
    test_schema: str,
    cleanup_unity_tables
):
    """
    Test Excel file preview, analysis, and parsing workflow.
    
    Flow:
    1. Preview Excel file via GET /api/excel/preview
    2. User selects header row from preview
    3. Analyze columns via GET /api/excel/analyze-columns
    4. User selects columns to include
    5. Parse to Delta via POST /api/excel/parse
    6. Verify table created with correct schema
    
    SKIPPED: Requires Lakeflow job with ingested Excel file.
    """
    connection_id = "existing_connection"
    file_path = "existing_file.xlsx"
    
    # Step 1: Preview
    preview_response = test_client.get(
        "/api/excel/preview",
        params={"connection_id": connection_id, "file_path": file_path, "max_rows": 100}
    )
    assert preview_response.status_code == 200
    preview = preview_response.json()
    assert "raw_data" in preview
    assert "sheets" in preview
    
    # Step 2: Analyze columns with header row
    analyze_response = test_client.get(
        "/api/excel/analyze-columns",
        params={"connection_id": connection_id, "file_path": file_path, "header_row": 0}
    )
    assert analyze_response.status_code == 200
    analysis = analyze_response.json()
    assert "columns" in analysis
    
    # Step 3: Parse to Delta
    cleanup_unity_tables("test_excel_parse")
    
    parse_request = {
        "connection_id": connection_id,
        "file_path": file_path,
        "table_name": "test_excel_parse",
        "header_row": 0,
        "selected_columns": ["col1", "col2"]
    }
    
    parse_response = test_client.post("/api/excel/parse", json=parse_request)
    assert parse_response.status_code == 200
    result = parse_response.json()
    assert "table_name" in result
    assert "rows_inserted" in result


def test_catalog_discovery_workflow(test_client: TestClient, test_catalog: str, test_schema: str):
    """Test catalog discovery and schema validation workflow."""
    # Step 1: Discover tables in schema
    discover_response = test_client.get(
        f"/api/catalog/catalogs/{test_catalog}/schemas/{test_schema}/tables",
        params={"include_stats": False}
    )
    assert discover_response.status_code == 200
    result = discover_response.json()
    assert "tables" in result
    assert "table_count" in result
    
    # Step 2: If tables exist, get schema for first one
    if result["table_count"] > 0:
        table_name = result["tables"][0]["name"]
        
        schema_response = test_client.get(
            f"/api/catalog/catalogs/{test_catalog}/schemas/{test_schema}/tables/{table_name}/schema"
        )
        assert schema_response.status_code == 200
        schema_result = schema_response.json()
        assert "columns" in schema_result
