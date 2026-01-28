"""
Test excel_parser service (services/excel_parser.py).
Tests Excel parsing from documents table to Delta tables.

NOTE: Tests skipped - Uses Lakebase which is not used in this deployment.
"""
import pytest
from app.services.excel_parser import parse_excel_to_delta
from app.core.models import SyncConfig

pytestmark = pytest.mark.skip(reason="Uses Lakebase which is not deployed")


@pytest.mark.skip(reason="Requires documents table with Excel file")
def test_parse_excel_to_delta_success(sample_sync_config: SyncConfig, cleanup_lakebase_tables):
    """
    Test parse_excel_to_delta() successfully parses Excel file.
    SKIPPED: Requires documents table with Excel file.
    """
    cleanup_lakebase_tables(sample_sync_config.target_table)
    
    result = parse_excel_to_delta(sample_sync_config)
    
    assert result["status"] == "success"
    assert "rows_processed" in result
    assert "supplier_id" in result
    assert "columns" in result


def test_parse_excel_to_delta_file_not_found(lakebase_catalog: str, lakebase_schema: str):
    """Test parse_excel_to_delta() handles missing file."""
    config = SyncConfig(
        id="test_missing_file",
        catalog=lakebase_catalog,
        schema_name=lakebase_schema,
        file_name="nonexistent_file.xlsx",
        documents_table="nonexistent_documents",
        target_table="test_target"
    )
    
    result = parse_excel_to_delta(config)
    
    assert result["status"] == "error"
    assert "not found" in result["message"].lower() or "error" in result["message"].lower()
    assert result["rows_processed"] == 0


@pytest.mark.skip(reason="Requires documents table")
def test_parse_excel_to_delta_table_not_found(sample_sync_config: SyncConfig):
    """
    Test parse_excel_to_delta() handles missing documents table.
    SKIPPED: Requires specific error condition setup.
    """
    config = SyncConfig(
        id=sample_sync_config.id,
        catalog=sample_sync_config.catalog,
        schema_name=sample_sync_config.schema_name,
        file_name=sample_sync_config.file_name,
        documents_table="nonexistent_documents_table",
        target_table=sample_sync_config.target_table
    )
    
    result = parse_excel_to_delta(config)
    
    assert result["status"] == "error"
    assert result["rows_processed"] == 0


@pytest.mark.skip(reason="Requires documents table with empty Excel file")
def test_parse_excel_to_delta_empty_file(sample_sync_config: SyncConfig, cleanup_lakebase_tables):
    """
    Test parse_excel_to_delta() handles empty Excel file.
    SKIPPED: Requires documents table with empty Excel file.
    """
    cleanup_lakebase_tables(sample_sync_config.target_table)
    
    result = parse_excel_to_delta(sample_sync_config)
    
    assert result["status"] == "success"
    assert result["rows_processed"] == 0
    assert "empty table" in result["message"].lower()


@pytest.mark.skip(reason="Requires documents table with malformed Excel")
def test_parse_excel_to_delta_malformed_excel(sample_sync_config: SyncConfig):
    """
    Test parse_excel_to_delta() handles malformed Excel file.
    SKIPPED: Requires documents table with malformed Excel file.
    """
    result = parse_excel_to_delta(sample_sync_config)
    
    assert result["status"] == "error"
    assert "error" in result["message"].lower()
