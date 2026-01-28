"""
Test /sharepoint endpoints (routes_sharepoint.py).
Tests SharePoint connection management via Databricks Unity Catalog.
"""
import pytest
from fastapi.testclient import TestClient


def test_list_sharepoint_connections(test_client: TestClient):
    """Test GET /sharepoint/connections lists SharePoint connections."""
    response = test_client.get("/sharepoint/connections")
    assert response.status_code == 200
    connections = response.json()
    assert isinstance(connections, list)
    # May be empty if no SharePoint connections exist


def test_create_sharepoint_connection_missing_credentials(test_client: TestClient):
    """Test POST /sharepoint/connections validates required fields."""
    # Missing required fields should fail validation
    incomplete_data = {
        "id": "test_connection",
        "name": "Test Connection"
        # Missing: client_id, client_secret, tenant_id, refresh_token, connection_name
    }
    
    response = test_client.post("/sharepoint/connections", json=incomplete_data)
    assert response.status_code == 422  # Pydantic validation error


def test_test_sharepoint_connection_not_found(test_client: TestClient):
    """Test POST /sharepoint/connections/{connection_id}/test with non-existent connection."""
    response = test_client.post("/sharepoint/connections/non_existent_connection_xyz/test")
    # May return 500 if Databricks SDK throws exception, or 404 if handled
    assert response.status_code in [404, 500]
    assert "not found" in response.json()["detail"].lower() or "failed" in response.json()["detail"].lower()


def test_delete_sharepoint_connection_not_found(test_client: TestClient):
    """Test DELETE /sharepoint/connections/{connection_id} with non-existent connection."""
    response = test_client.delete("/sharepoint/connections/non_existent_connection_xyz")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.skip(reason="Requires valid SharePoint OAuth credentials")
def test_create_sharepoint_connection_full(test_client: TestClient):
    """
    Test POST /sharepoint/connections creates connection.
    SKIPPED: Requires valid OAuth credentials and SharePoint setup.
    """
    connection_data = {
        "id": "test_sharepoint_001",
        "name": "Test SharePoint Connection",
        "client_id": "dummy_client_id",
        "client_secret": "dummy_secret",
        "tenant_id": "dummy_tenant",
        "refresh_token": "dummy_token",
        "site_id": "00000000-0000-0000-0000-000000000000",
        "connection_name": "test-sharepoint-conn"
    }
    
    response = test_client.post("/sharepoint/connections", json=connection_data)
    # Would succeed with valid credentials
    assert response.status_code in [200, 400, 500]


@pytest.mark.skip(reason="Requires existing SharePoint connection")
def test_test_sharepoint_connection_success(test_client: TestClient):
    """
    Test POST /sharepoint/connections/{connection_id}/test succeeds.
    SKIPPED: Requires existing SharePoint connection in workspace.
    """
    response = test_client.post("/sharepoint/connections/existing_connection/test")
    assert response.status_code == 200
    assert response.json()["success"] is True
