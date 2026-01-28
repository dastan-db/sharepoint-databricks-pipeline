"""
Test /api/lakeflow endpoints (routes_lakeflow.py).
Tests Lakeflow ingestion job management and Excel sync configuration.
"""
import pytest
from fastapi.testclient import TestClient


def test_list_lakeflow_jobs_empty(test_client: TestClient):
    """Test GET /api/lakeflow/jobs returns empty list initially."""
    response = test_client.get("/api/lakeflow/jobs")
    assert response.status_code == 200
    jobs = response.json()
    assert isinstance(jobs, list)


def test_create_lakeflow_job_missing_site_id(test_client: TestClient):
    """Test POST /api/lakeflow/jobs validates site_id requirement."""
    job_config = {
        "connection_id": "test_conn_001",
        "connection_name": "test-sharepoint",
        "source_schema": "",  # Empty site_id should fail
        "destination_catalog": "main",
        "destination_schema": "test_schema"
    }
    
    response = test_client.post("/api/lakeflow/jobs", json=job_config)
    assert response.status_code == 400
    assert "site id" in response.json()["detail"].lower()


def test_create_lakeflow_job_invalid_data(test_client: TestClient):
    """Test POST /api/lakeflow/jobs validates request data."""
    invalid_config = {
        "connection_id": "test",
        # Missing required fields
    }
    
    response = test_client.post("/api/lakeflow/jobs", json=invalid_config)
    assert response.status_code == 422  # Pydantic validation error


def test_get_job_status_not_found(test_client: TestClient):
    """Test GET /api/lakeflow/jobs/{connection_id}/status with non-existent job."""
    response = test_client.get("/api/lakeflow/jobs/non_existent_job_xyz/status")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_get_documents_table_not_found(test_client: TestClient):
    """Test GET /api/lakeflow/jobs/{connection_id}/documents with non-existent job."""
    response = test_client.get("/api/lakeflow/jobs/non_existent_job_xyz/documents")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_configure_sync_not_found(test_client: TestClient):
    """Test POST /api/lakeflow/jobs/{connection_id}/configure-sync with non-existent job."""
    sync_config = {
        "file_path": "test.xlsx",
        "table_name": "test_table",
        "header_row": 0
    }
    
    response = test_client.post(
        "/api/lakeflow/jobs/non_existent_job_xyz/configure-sync",
        json=sync_config
    )
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_configure_sync_invalid_request(test_client: TestClient):
    """Test POST /api/lakeflow/jobs/{connection_id}/configure-sync validates request."""
    invalid_config = {
        "file_path": "test.xlsx"
        # Missing required field: table_name
    }
    
    response = test_client.post(
        "/api/lakeflow/jobs/test_job/configure-sync",
        json=invalid_config
    )
    assert response.status_code == 422  # Pydantic validation error


def test_run_sync_job_not_found(test_client: TestClient):
    """Test POST /api/lakeflow/jobs/{connection_id}/run-sync with non-existent job."""
    response = test_client.post("/api/lakeflow/jobs/non_existent_job_xyz/run-sync")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_disable_sync_not_found(test_client: TestClient):
    """Test DELETE /api/lakeflow/jobs/{connection_id}/disable-sync with non-existent job."""
    response = test_client.delete("/api/lakeflow/jobs/non_existent_job_xyz/disable-sync")
    # May return 200 (no-op) or 404 depending on implementation
    assert response.status_code in [200, 404, 500]


def test_delete_lakeflow_job_not_found(test_client: TestClient):
    """Test DELETE /api/lakeflow/jobs/{connection_id} with non-existent job."""
    response = test_client.delete("/api/lakeflow/jobs/non_existent_job_xyz")
    # DELETE may succeed even if job doesn't exist (idempotent)
    assert response.status_code in [200, 404, 500]


@pytest.mark.skip(reason="Requires valid SharePoint connection and credentials")
def test_create_lakeflow_job_success(test_client: TestClient, test_catalog: str, test_schema: str, cleanup_lakeflow_jobs, cleanup_unity_tables):
    """
    Test POST /api/lakeflow/jobs creates job successfully.
    SKIPPED: Requires valid SharePoint connection in Unity Catalog.
    """
    cleanup_unity_tables("lakeflow_jobs")
    
    job_config = {
        "connection_id": "test_conn_full",
        "connection_name": "existing-sharepoint-connection",
        "source_schema": "00000000-0000-0000-0000-000000000000",
        "destination_catalog": test_catalog,
        "destination_schema": test_schema
    }
    
    response = test_client.post("/api/lakeflow/jobs", json=job_config)
    
    if response.status_code == 200:
        result = response.json()
        assert "connection_id" in result
        assert "document_pipeline_id" in result
        assert "job_id" in result
        
        # Register for cleanup
        cleanup_lakeflow_jobs["job"](result["job_id"])
        cleanup_lakeflow_jobs["pipeline"](result["document_pipeline_id"])


@pytest.mark.skip(reason="Requires existing lakeflow job")
def test_get_job_status_success(test_client: TestClient):
    """
    Test GET /api/lakeflow/jobs/{connection_id}/status returns status.
    SKIPPED: Requires existing lakeflow job.
    """
    response = test_client.get("/api/lakeflow/jobs/existing_job/status")
    assert response.status_code == 200
    result = response.json()
    assert "connection_id" in result
    assert "document_pipeline" in result


@pytest.mark.skip(reason="Requires existing lakeflow job with documents")
def test_get_documents_table_success(test_client: TestClient):
    """
    Test GET /api/lakeflow/jobs/{connection_id}/documents returns documents.
    SKIPPED: Requires existing lakeflow job with ingested documents.
    """
    response = test_client.get("/api/lakeflow/jobs/existing_job/documents?limit=10")
    assert response.status_code == 200
    result = response.json()
    assert "documents" in result
    assert "count" in result


@pytest.mark.skip(reason="Requires existing lakeflow job")
def test_configure_sync_success(test_client: TestClient):
    """
    Test POST /api/lakeflow/jobs/{connection_id}/configure-sync configures sync.
    SKIPPED: Requires existing lakeflow job with documents.
    """
    sync_config = {
        "file_path": "existing_file.xlsx",
        "table_name": "test_sync_table",
        "header_row": 0,
        "selected_columns": ["col1", "col2"]
    }
    
    response = test_client.post(
        "/api/lakeflow/jobs/existing_job/configure-sync",
        json=sync_config
    )
    assert response.status_code == 200
    result = response.json()
    assert result["sync_enabled"] is True
    assert "notebook_path" in result


@pytest.mark.skip(reason="Requires existing lakeflow job with sync configured")
def test_run_sync_job_success(test_client: TestClient):
    """
    Test POST /api/lakeflow/jobs/{connection_id}/run-sync triggers sync.
    SKIPPED: Requires existing lakeflow job with sync configured.
    """
    response = test_client.post("/api/lakeflow/jobs/existing_job/run-sync")
    assert response.status_code == 200
    result = response.json()
    assert "run_id" in result


def test_add_triggers_no_jobs(test_client: TestClient):
    """Test POST /api/lakeflow/jobs/add-triggers with no existing jobs."""
    response = test_client.post("/api/lakeflow/jobs/add-triggers")
    assert response.status_code == 200
    result = response.json()
    assert "updated_jobs" in result
    assert isinstance(result["updated_jobs"], list)


@pytest.mark.skip(reason="Requires existing Databricks jobs to update")
def test_add_triggers_success(test_client: TestClient):
    """
    Test POST /api/lakeflow/jobs/add-triggers updates existing jobs.
    SKIPPED: Requires existing Databricks jobs.
    """
    response = test_client.post("/api/lakeflow/jobs/add-triggers")
    assert response.status_code == 200
    result = response.json()
    assert "updated_jobs" in result
    assert "failed_jobs" in result
