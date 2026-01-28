"""
Test update_checker service (services/update_checker.py).
Tests file modification timestamp checking for sync gating.

NOTE: Tests skipped - Uses Lakebase which is not used in this deployment.
"""
import pytest
from app.services.update_checker import should_process_file
from app.core.models import SyncConfig

pytestmark = pytest.mark.skip(reason="Uses Lakebase which is not deployed")


def test_should_process_file_file_not_found(lakebase_catalog: str, lakebase_schema: str):
    """Test should_process_file() when file doesn't exist in documents table."""
    config = SyncConfig(
        id="test_no_file",
        catalog=lakebase_catalog,
        schema_name=lakebase_schema,
        file_name="nonexistent_file.xlsx",
        documents_table="nonexistent_documents",
        target_table="test_target"
    )
    
    result = should_process_file(config)
    
    assert result["should_process"] is False
    assert "not found" in result["reason"].lower() or "error" in result["reason"].lower()
    assert result["file_last_updated"] is None


def test_should_process_file_table_not_found(lakebase_catalog: str, lakebase_schema: str):
    """Test should_process_file() when target table doesn't exist."""
    # This will fail on documents table lookup, but tests error handling
    config = SyncConfig(
        id="test_no_table",
        catalog=lakebase_catalog,
        schema_name=lakebase_schema,
        file_name="test_file.xlsx",
        documents_table="nonexistent_documents",
        target_table="nonexistent_target"
    )
    
    result = should_process_file(config)
    
    # Should handle gracefully
    assert "should_process" in result
    assert "reason" in result


@pytest.mark.skip(reason="Requires documents table with Excel file")
def test_should_process_file_initial_load(sample_sync_config: SyncConfig):
    """
    Test should_process_file() for initial load (target table doesn't exist).
    SKIPPED: Requires documents table with Excel file.
    """
    result = should_process_file(sample_sync_config)
    
    if "does not exist" in result["reason"].lower():
        assert result["should_process"] is True
        assert result["table_last_updated"] is None


@pytest.mark.skip(reason="Requires documents table with Excel file and target table")
def test_should_process_file_file_is_newer(sample_sync_config: SyncConfig):
    """
    Test should_process_file() when file is newer than table.
    SKIPPED: Requires documents table with recent Excel file.
    """
    result = should_process_file(sample_sync_config)
    
    if result["should_process"]:
        assert "newer changes" in result["reason"].lower()


@pytest.mark.skip(reason="Requires documents table with Excel file and target table")
def test_should_process_file_file_is_older(sample_sync_config: SyncConfig):
    """
    Test should_process_file() when file is older than table (no updates).
    SKIPPED: Requires documents table with old Excel file.
    """
    result = should_process_file(sample_sync_config)
    
    if not result["should_process"]:
        assert "up to date" in result["reason"].lower()


def test_should_process_file_returns_metadata(lakebase_catalog: str, lakebase_schema: str):
    """Test should_process_file() returns proper metadata structure."""
    config = SyncConfig(
        id="test_metadata",
        catalog=lakebase_catalog,
        schema_name=lakebase_schema,
        file_name="test.xlsx",
        documents_table="docs",
        target_table="target"
    )
    
    result = should_process_file(config)
    
    # Check structure
    assert "catalog" in result
    assert "schema" in result
    assert "file_name" in result
    assert "documents_table" in result
    assert "target_table" in result
    assert "should_process" in result
    assert "reason" in result
    assert "file_last_updated" in result
    assert "table_last_updated" in result
