"""
Test pipeline orchestration (core/pipeline.py).
Tests the run_sync() function that orchestrates gate -> parse -> DQ.

NOTE: Tests skipped - Uses Lakebase which is not used in this deployment.
"""
import pytest
from app.core.pipeline import run_sync
from app.core.models import SyncConfig

pytestmark = pytest.mark.skip(reason="Uses Lakebase which is not deployed")


@pytest.mark.skip(reason="Requires full setup with documents table and Excel file")
def test_run_sync_skipped_status(sample_sync_config: SyncConfig):
    """
    Test run_sync() returns 'skipped' when file hasn't changed.
    SKIPPED: Requires documents table with Excel file that hasn't been updated.
    """
    result = run_sync(sample_sync_config)
    
    assert result.status in ["skipped", "error"]
    assert result.gate is not None
    
    if result.status == "skipped":
        assert result.gate["should_process"] is False


@pytest.mark.skip(reason="Requires full setup with documents table and Excel file")
def test_run_sync_success_status(sample_sync_config: SyncConfig):
    """
    Test run_sync() returns 'success' with valid data.
    SKIPPED: Requires documents table with Excel file and target table.
    """
    result = run_sync(sample_sync_config)
    
    assert result.status in ["success", "dq_failed", "error"]
    assert result.gate is not None
    
    if result.status == "success":
        assert result.parse is not None
        assert result.dq is not None
        assert result.dq["checks_passed"] is True


@pytest.mark.skip(reason="Requires full setup with documents table and Excel file")
def test_run_sync_dq_failed_status(sample_sync_config: SyncConfig):
    """
    Test run_sync() returns 'dq_failed' when data quality checks fail.
    SKIPPED: Requires documents table with Excel file that has bad data.
    """
    result = run_sync(sample_sync_config)
    
    if result.status == "dq_failed":
        assert result.parse is not None
        assert result.dq is not None
        assert result.dq["checks_passed"] is False


@pytest.mark.skip(reason="Requires full setup")
def test_run_sync_error_status(sample_sync_config: SyncConfig):
    """
    Test run_sync() returns 'error' when processing fails.
    SKIPPED: Requires setup that will trigger errors.
    """
    result = run_sync(sample_sync_config)
    
    if result.status == "error":
        assert result.gate is not None


def test_run_sync_with_nonexistent_file(lakebase_catalog: str, lakebase_schema: str):
    """Test run_sync() handles non-existent file gracefully."""
    config = SyncConfig(
        id="test_nonexistent",
        catalog=lakebase_catalog,
        schema_name=lakebase_schema,
        file_name="nonexistent_file.xlsx",
        documents_table="nonexistent_documents",
        target_table="nonexistent_target"
    )
    
    result = run_sync(config)
    
    # Should return skipped or error (gate should catch it)
    assert result.status in ["skipped", "error"]
    assert result.gate is not None
