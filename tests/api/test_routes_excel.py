"""
Test /api/excel endpoints (routes_excel.py).
Tests Excel file preview, analysis, and parsing to Delta tables.
"""
import pytest
from fastapi.testclient import TestClient


def test_preview_excel_missing_connection(test_client: TestClient):
    """Test GET /api/excel/preview with non-existent connection."""
    response = test_client.get(
        "/api/excel/preview",
        params={
            "connection_id": "non_existent_connection",
            "file_path": "dummy.xlsx"
        }
    )
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_preview_excel_missing_file(test_client: TestClient):
    """Test GET /api/excel/preview with non-existent file."""
    # This will fail because connection doesn't exist, but tests the error handling
    response = test_client.get(
        "/api/excel/preview",
        params={
            "connection_id": "test_connection",
            "file_path": "non_existent_file.xlsx"
        }
    )
    assert response.status_code in [404, 500]


def test_analyze_columns_missing_connection(test_client: TestClient):
    """Test GET /api/excel/analyze-columns with non-existent connection."""
    response = test_client.get(
        "/api/excel/analyze-columns",
        params={
            "connection_id": "non_existent_connection",
            "file_path": "dummy.xlsx",
            "header_row": 0
        }
    )
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_parse_excel_missing_connection(test_client: TestClient):
    """Test POST /api/excel/parse with non-existent connection."""
    parse_request = {
        "connection_id": "non_existent_connection",
        "file_path": "dummy.xlsx",
        "table_name": "test_table",
        "header_row": 0
    }
    
    response = test_client.post("/api/excel/parse", json=parse_request)
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_parse_excel_invalid_request(test_client: TestClient):
    """Test POST /api/excel/parse with invalid request data."""
    invalid_request = {
        "connection_id": "test",
        # Missing required fields: file_path, table_name
    }
    
    response = test_client.post("/api/excel/parse", json=invalid_request)
    assert response.status_code == 422  # Pydantic validation error


@pytest.mark.skip(reason="Requires lakeflow job with documents table and Excel file")
def test_preview_excel_success(test_client: TestClient):
    """
    Test GET /api/excel/preview returns file preview.
    SKIPPED: Requires lakeflow job with ingested Excel file.
    """
    response = test_client.get(
        "/api/excel/preview",
        params={
            "connection_id": "existing_connection",
            "file_path": "existing_file.xlsx",
            "max_rows": 100
        }
    )
    assert response.status_code == 200
    result = response.json()
    assert "sheets" in result
    assert "raw_data" in result
    assert "recommended_table_name" in result


@pytest.mark.skip(reason="Requires lakeflow job with documents table and Excel file")
def test_analyze_columns_success(test_client: TestClient):
    """
    Test GET /api/excel/analyze-columns returns column metadata.
    SKIPPED: Requires lakeflow job with ingested Excel file.
    """
    response = test_client.get(
        "/api/excel/analyze-columns",
        params={
            "connection_id": "existing_connection",
            "file_path": "existing_file.xlsx",
            "header_row": 0
        }
    )
    assert response.status_code == 200
    result = response.json()
    assert "columns" in result
    assert "row_count" in result
    assert "header_row" in result


@pytest.mark.skip(reason="Requires lakeflow job with documents table and Excel file")
def test_parse_excel_success(test_client: TestClient, test_catalog: str, test_schema: str, cleanup_unity_tables):
    """
    Test POST /api/excel/parse creates Delta table.
    SKIPPED: Requires lakeflow job with ingested Excel file.
    """
    cleanup_unity_tables("test_parsed_excel")
    
    parse_request = {
        "connection_id": "existing_connection",
        "file_path": "existing_file.xlsx",
        "table_name": "test_parsed_excel",
        "header_row": 0
    }
    
    response = test_client.post("/api/excel/parse", json=parse_request)
    assert response.status_code == 200
    result = response.json()
    assert "table_name" in result
    assert "rows_inserted" in result
    assert "columns" in result
