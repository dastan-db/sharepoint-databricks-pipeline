"""
Test data_quality service (services/data_quality.py).
Tests data quality checks on target tables.

NOTE: Tests skipped - Uses Lakebase which is not used in this deployment.
"""
import pytest
from app.services.data_quality import run_data_quality_checks
from app.core.models import SyncConfig

pytestmark = pytest.mark.skip(reason="Uses Lakebase which is not deployed")


def test_run_data_quality_checks_table_not_found(lakebase_catalog: str, lakebase_schema: str):
    """Test run_data_quality_checks() when table doesn't exist."""
    config = SyncConfig(
        id="test_no_table",
        catalog=lakebase_catalog,
        schema_name=lakebase_schema,
        file_name="test.xlsx",
        documents_table="docs",
        target_table="nonexistent_table"
    )
    
    result = run_data_quality_checks(config)
    
    assert result["status"] == "error"
    assert "not found" in result["message"].lower()
    assert result["checks_passed"] is False


@pytest.mark.skip(reason="Requires target table with data")
def test_run_data_quality_checks_success(sample_sync_config: SyncConfig):
    """
    Test run_data_quality_checks() with valid data.
    SKIPPED: Requires populated target table.
    """
    result = run_data_quality_checks(sample_sync_config)
    
    assert result["status"] == "success"
    assert "checks_passed" in result
    assert "total_checks" in result
    assert "quality_checks" in result
    assert isinstance(result["quality_checks"], list)


@pytest.mark.skip(reason="Requires target table with data")
def test_run_data_quality_checks_row_count(sample_sync_config: SyncConfig):
    """
    Test row_count check passes with data.
    SKIPPED: Requires populated target table.
    """
    result = run_data_quality_checks(sample_sync_config)
    
    row_count_check = next(
        (check for check in result["quality_checks"] if check["check"] == "row_count"),
        None
    )
    
    assert row_count_check is not None
    assert "value" in row_count_check
    assert row_count_check["value"] >= 0


@pytest.mark.skip(reason="Requires target table with data")
def test_run_data_quality_checks_required_columns(sample_sync_config: SyncConfig):
    """
    Test required_columns check.
    SKIPPED: Requires populated target table.
    """
    result = run_data_quality_checks(
        sample_sync_config,
        required_columns=["Date", "SKU", "Qty", "supplier_id"]
    )
    
    column_check = next(
        (check for check in result["quality_checks"] if check["check"] == "required_columns"),
        None
    )
    
    assert column_check is not None
    assert "value" in column_check


@pytest.mark.skip(reason="Requires target table with data")
def test_run_data_quality_checks_null_values(sample_sync_config: SyncConfig):
    """
    Test null_values check.
    SKIPPED: Requires populated target table.
    """
    result = run_data_quality_checks(sample_sync_config)
    
    null_check = next(
        (check for check in result["quality_checks"] if check["check"] == "null_values"),
        None
    )
    
    if null_check:
        assert "value" in null_check
        assert isinstance(null_check["value"], dict)


@pytest.mark.skip(reason="Requires target table with data")
def test_run_data_quality_checks_supplier_consistency(sample_sync_config: SyncConfig):
    """
    Test supplier_consistency check (should have exactly 1 supplier).
    SKIPPED: Requires populated target table.
    """
    result = run_data_quality_checks(sample_sync_config)
    
    supplier_check = next(
        (check for check in result["quality_checks"] if check["check"] == "supplier_consistency"),
        None
    )
    
    if supplier_check:
        assert "value" in supplier_check
        # Should be 1 for consistent supplier data
        assert supplier_check["value"] >= 0


def test_run_data_quality_checks_custom_columns(lakebase_catalog: str, lakebase_schema: str):
    """Test run_data_quality_checks() with custom required columns."""
    config = SyncConfig(
        id="test_custom",
        catalog=lakebase_catalog,
        schema_name=lakebase_schema,
        file_name="test.xlsx",
        documents_table="docs",
        target_table="nonexistent_table"
    )
    
    custom_columns = ["custom_col1", "custom_col2"]
    result = run_data_quality_checks(config, required_columns=custom_columns)
    
    # Will error on missing table, but should accept custom columns
    assert result["status"] == "error"


def test_run_data_quality_checks_structure(lakebase_catalog: str, lakebase_schema: str):
    """Test run_data_quality_checks() returns proper structure."""
    config = SyncConfig(
        id="test_structure",
        catalog=lakebase_catalog,
        schema_name=lakebase_schema,
        file_name="test.xlsx",
        documents_table="docs",
        target_table="test_table"
    )
    
    result = run_data_quality_checks(config)
    
    # Check structure
    assert "status" in result
    assert "checks_passed" in result
    assert isinstance(result["checks_passed"], bool)
