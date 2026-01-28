"""
Test Pydantic models (core/models.py).
Tests model validation and serialization.
"""
import pytest
from pydantic import ValidationError
from app.core.models import SyncConfig, RunResult, LakeflowJobConfig


# ============================================
# SyncConfig Tests
# ============================================

def test_sync_config_valid():
    """Test SyncConfig with valid data."""
    config = SyncConfig(
        id="test_config",
        catalog="main",
        schema_name="test_schema",
        file_name="test.xlsx",
        documents_table="docs",
        target_table="target"
    )
    
    assert config.id == "test_config"
    assert config.catalog == "main"
    assert config.schema_name == "test_schema"
    assert config.file_name == "test.xlsx"
    assert config.documents_table == "docs"
    assert config.target_table == "target"


def test_sync_config_defaults():
    """Test SyncConfig uses default values."""
    config = SyncConfig(
        id="test",
        catalog="main",
        schema_name="test",
        file_name="test.xlsx"
    )
    
    # Should have default values
    assert config.documents_table == "documents"
    assert config.target_table == "supplier_a_data"


def test_sync_config_missing_required_fields():
    """Test SyncConfig validates required fields."""
    with pytest.raises(ValidationError):
        SyncConfig(
            id="test",
            catalog="main"
            # Missing required: schema_name, file_name
        )


def test_sync_config_dict_serialization():
    """Test SyncConfig serializes to dict."""
    config = SyncConfig(
        id="test",
        catalog="main",
        schema_name="test",
        file_name="test.xlsx"
    )
    
    config_dict = config.dict()
    assert isinstance(config_dict, dict)
    assert config_dict["id"] == "test"
    assert config_dict["catalog"] == "main"


def test_sync_config_json_serialization():
    """Test SyncConfig serializes to JSON."""
    config = SyncConfig(
        id="test",
        catalog="main",
        schema_name="test",
        file_name="test.xlsx"
    )
    
    json_str = config.json()
    assert isinstance(json_str, str)
    assert "test" in json_str
    assert "main" in json_str


# ============================================
# RunResult Tests
# ============================================

def test_run_result_skipped_status():
    """Test RunResult with 'skipped' status."""
    result = RunResult(
        status="skipped",
        gate={"should_process": False, "reason": "No updates"}
    )
    
    assert result.status == "skipped"
    assert result.gate["should_process"] is False
    assert result.parse is None
    assert result.dq is None


def test_run_result_success_status():
    """Test RunResult with 'success' status."""
    result = RunResult(
        status="success",
        gate={"should_process": True},
        parse={"status": "success", "rows_processed": 10},
        dq={"checks_passed": True}
    )
    
    assert result.status == "success"
    assert result.gate["should_process"] is True
    assert result.parse["rows_processed"] == 10
    assert result.dq["checks_passed"] is True


def test_run_result_dq_failed_status():
    """Test RunResult with 'dq_failed' status."""
    result = RunResult(
        status="dq_failed",
        gate={"should_process": True},
        parse={"status": "success"},
        dq={"checks_passed": False}
    )
    
    assert result.status == "dq_failed"
    assert result.dq["checks_passed"] is False


def test_run_result_error_status():
    """Test RunResult with 'error' status."""
    result = RunResult(
        status="error",
        gate={"should_process": True},
        parse={"status": "error", "message": "Failed"}
    )
    
    assert result.status == "error"
    assert result.parse["status"] == "error"


def test_run_result_invalid_status():
    """Test RunResult validates status values."""
    with pytest.raises(ValidationError):
        RunResult(
            status="invalid_status",  # Not in allowed values
            gate={}
        )


def test_run_result_missing_gate():
    """Test RunResult requires gate field."""
    with pytest.raises(ValidationError):
        RunResult(status="skipped")  # Missing gate


def test_run_result_dict_serialization():
    """Test RunResult serializes to dict."""
    result = RunResult(
        status="success",
        gate={"should_process": True},
        parse={"rows": 5},
        dq={"passed": True}
    )
    
    result_dict = result.dict()
    assert isinstance(result_dict, dict)
    assert result_dict["status"] == "success"
    assert result_dict["gate"]["should_process"] is True


# ============================================
# LakeflowJobConfig Tests
# ============================================

def test_lakeflow_job_config_valid():
    """Test LakeflowJobConfig with valid data."""
    config = LakeflowJobConfig(
        connection_id="conn_123",
        connection_name="test-sharepoint",
        source_schema="00000000-0000-0000-0000-000000000000",
        destination_catalog="main",
        destination_schema="test_schema"
    )
    
    assert config.connection_id == "conn_123"
    assert config.connection_name == "test-sharepoint"
    assert config.destination_catalog == "main"
    assert config.sync_enabled is False  # Default


def test_lakeflow_job_config_with_optional_fields():
    """Test LakeflowJobConfig with all optional fields."""
    config = LakeflowJobConfig(
        connection_id="conn_123",
        connection_name="test-sharepoint",
        source_schema="site_id",
        destination_catalog="main",
        destination_schema="test",
        document_pipeline_id="pipeline_123",
        document_table="main.test.docs",
        created_at="2024-01-01T00:00:00",
        job_id="job_456",
        tracked_file_path="file.xlsx",
        target_table="main.test.target",
        sync_enabled=True
    )
    
    assert config.document_pipeline_id == "pipeline_123"
    assert config.document_table == "main.test.docs"
    assert config.job_id == "job_456"
    assert config.tracked_file_path == "file.xlsx"
    assert config.target_table == "main.test.target"
    assert config.sync_enabled is True


def test_lakeflow_job_config_defaults():
    """Test LakeflowJobConfig default values."""
    config = LakeflowJobConfig(
        connection_id="conn_123",
        connection_name="test-sharepoint",
        source_schema="site_id",
        destination_catalog="main",
        destination_schema="test"
    )
    
    assert config.document_pipeline_id is None
    assert config.document_table is None
    assert config.created_at is None
    assert config.job_id is None
    assert config.tracked_file_path is None
    assert config.target_table is None
    assert config.sync_enabled is False


def test_lakeflow_job_config_missing_required_fields():
    """Test LakeflowJobConfig validates required fields."""
    with pytest.raises(ValidationError):
        LakeflowJobConfig(
            connection_id="conn_123",
            connection_name="test"
            # Missing: source_schema, destination_catalog, destination_schema
        )


def test_lakeflow_job_config_dict_serialization():
    """Test LakeflowJobConfig serializes to dict."""
    config = LakeflowJobConfig(
        connection_id="conn_123",
        connection_name="test-sharepoint",
        source_schema="site_id",
        destination_catalog="main",
        destination_schema="test"
    )
    
    config_dict = config.dict()
    assert isinstance(config_dict, dict)
    assert config_dict["connection_id"] == "conn_123"
    assert config_dict["sync_enabled"] is False


def test_lakeflow_job_config_json_serialization():
    """Test LakeflowJobConfig serializes to JSON."""
    config = LakeflowJobConfig(
        connection_id="conn_123",
        connection_name="test-sharepoint",
        source_schema="site_id",
        destination_catalog="main",
        destination_schema="test"
    )
    
    json_str = config.json()
    assert isinstance(json_str, str)
    assert "conn_123" in json_str
    assert "test-sharepoint" in json_str


def test_lakeflow_job_config_sync_enabled_type():
    """Test LakeflowJobConfig sync_enabled is boolean."""
    config = LakeflowJobConfig(
        connection_id="conn_123",
        connection_name="test",
        source_schema="site",
        destination_catalog="main",
        destination_schema="test",
        sync_enabled=True
    )
    
    assert isinstance(config.sync_enabled, bool)
    assert config.sync_enabled is True


def test_lakeflow_job_config_from_dict():
    """Test creating LakeflowJobConfig from dictionary."""
    data = {
        "connection_id": "conn_123",
        "connection_name": "test-sharepoint",
        "source_schema": "site_id",
        "destination_catalog": "main",
        "destination_schema": "test",
        "sync_enabled": False
    }
    
    config = LakeflowJobConfig(**data)
    assert config.connection_id == "conn_123"
    assert config.sync_enabled is False
