"""
Test FastAPI application initialization and basic endpoints.
"""
import pytest
from fastapi.testclient import TestClient


def test_app_initialization(test_client: TestClient):
    """Test that the FastAPI app initializes correctly."""
    assert test_client.app is not None
    assert test_client.app.title == "SharePoint to Databricks Data Pipeline"


def test_health_endpoint(test_client: TestClient):
    """Test /health endpoint returns OK status."""
    response = test_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_root_endpoint(test_client: TestClient):
    """Test / endpoint serves HTML file."""
    response = test_client.get("/")
    assert response.status_code == 200
    # Should return HTML content
    assert response.headers["content-type"] in ["text/html; charset=utf-8", "text/html"]


def test_openapi_docs_available(test_client: TestClient):
    """Test that OpenAPI documentation is available."""
    response = test_client.get("/docs")
    assert response.status_code == 200


def test_openapi_json_available(test_client: TestClient):
    """Test that OpenAPI JSON spec is available."""
    response = test_client.get("/openapi.json")
    assert response.status_code == 200
    spec = response.json()
    assert "openapi" in spec
    assert "paths" in spec
