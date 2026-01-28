"""
Shared pytest fixtures for all tests.
Provides FastAPI TestClient, Databricks connections, and test data.
"""
import pytest
import os
import io
from typing import Generator
from fastapi.testclient import TestClient
from databricks.sdk import WorkspaceClient
import pandas as pd

# Import app
from app.main import app
from app.core.models import SyncConfig, LakeflowJobConfig
from app.services.unity_catalog import UnityCatalog


# ============================================
# Global Fixtures
# ============================================

@pytest.fixture(scope="session")
def test_client() -> TestClient:
    """FastAPI TestClient for endpoint testing."""
    return TestClient(app)


@pytest.fixture(scope="session")
def test_catalog() -> str:
    """Test catalog name from environment or default."""
    return os.getenv("TEST_CATALOG", os.getenv("UC_CATALOG", "main"))


@pytest.fixture(scope="session")
def test_schema() -> str:
    """Test schema name with _test suffix to avoid production data."""
    base_schema = os.getenv("TEST_SCHEMA", "test_vibe_app")
    return f"{base_schema}_test"




# ============================================
# Service Fixtures
# ============================================

@pytest.fixture(scope="session")
def workspace_client() -> WorkspaceClient:
    """Databricks Workspace Client for SDK operations."""
    return WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )


@pytest.fixture(scope="function")
def unity_catalog_connection():
    """Shared UnityCatalog instance (singleton)."""
    return UnityCatalog


# ============================================
# Data Fixtures
# ============================================

@pytest.fixture
def sample_excel_file() -> bytes:
    """
    Generate a sample Excel file for testing.
    Returns binary content that can be inserted into documents table.
    """
    # Create sample dataframe
    data = {
        'Date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'SKU': ['SKU001', 'SKU002', 'SKU003'],
        'Qty': [10, 20, 30],
        'Price': [100.0, 200.0, 300.0]
    }
    df = pd.DataFrame(data)
    
    # Write to Excel in memory
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        # Write supplier ID in B1 (row 0, col 1)
        temp_df = pd.DataFrame([['Supplier ID', 'SUPP001']])
        temp_df.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=0)
        
        # Write main data starting at row 3 (header row 2)
        df.to_excel(writer, sheet_name='Sheet1', index=False, startrow=2)
    
    buffer.seek(0)
    return buffer.read()


@pytest.fixture
def sample_sync_config(test_catalog: str, test_schema: str) -> SyncConfig:
    """Valid SyncConfig for testing (legacy - not actively used)."""
    return SyncConfig(
        id="test_supplier_a",
        catalog=test_catalog,
        schema_name=test_schema,
        file_name="test_supplier_a.xlsx",
        documents_table="test_documents",
        target_table="test_supplier_a_data"
    )


@pytest.fixture
def sample_lakeflow_config(test_catalog: str, test_schema: str) -> LakeflowJobConfig:
    """Valid LakeflowJobConfig for testing."""
    return LakeflowJobConfig(
        connection_id="test_connection_123",
        connection_name="test-sharepoint-connection",
        source_schema="00000000-0000-0000-0000-000000000000",  # Dummy site ID
        destination_catalog=test_catalog,
        destination_schema=test_schema,
        sync_enabled=False
    )


# ============================================
# Cleanup Fixtures
# ============================================


@pytest.fixture(scope="function")
def cleanup_unity_tables(unity_catalog_connection, test_catalog: str, test_schema: str):
    """
    Cleanup fixture for Unity Catalog tables.
    Yields, then drops test tables after test completes.
    """
    tables_to_cleanup = []
    
    def register_table(table_name: str):
        """Register a table for cleanup."""
        tables_to_cleanup.append(table_name)
    
    # Yield control to test
    yield register_table
    
    # Cleanup after test
    for table_name in tables_to_cleanup:
        try:
            full_name = f"{test_catalog}.{test_schema}.{table_name}"
            unity_catalog_connection.query(f"DROP TABLE IF EXISTS {full_name}")
            print(f"Cleaned up table: {full_name}")
        except Exception as e:
            print(f"Warning: Could not cleanup table {table_name}: {e}")


@pytest.fixture(scope="function")
def cleanup_lakeflow_jobs(workspace_client: WorkspaceClient):
    """
    Cleanup fixture for Databricks Jobs and Pipelines.
    Yields, then deletes resources after test completes.
    """
    jobs_to_cleanup = []
    pipelines_to_cleanup = []
    
    def register_job(job_id: str):
        """Register a job for cleanup."""
        jobs_to_cleanup.append(job_id)
    
    def register_pipeline(pipeline_id: str):
        """Register a pipeline for cleanup."""
        pipelines_to_cleanup.append(pipeline_id)
    
    # Yield control to test
    yield {"job": register_job, "pipeline": register_pipeline}
    
    # Cleanup after test
    for job_id in jobs_to_cleanup:
        try:
            workspace_client.jobs.delete(job_id=int(job_id))
            print(f"Cleaned up job: {job_id}")
        except Exception as e:
            print(f"Warning: Could not cleanup job {job_id}: {e}")
    
    for pipeline_id in pipelines_to_cleanup:
        try:
            workspace_client.pipelines.delete(pipeline_id=pipeline_id)
            print(f"Cleaned up pipeline: {pipeline_id}")
        except Exception as e:
            print(f"Warning: Could not cleanup pipeline {pipeline_id}: {e}")


# ============================================
# Session-level Setup/Teardown
# ============================================

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment(test_catalog: str, test_schema: str):
    """
    Setup test environment before all tests.
    Creates test schemas if they don't exist.
    """
    print("\n" + "="*60)
    print("Setting up test environment...")
    print("="*60)
    
    # Setup Unity Catalog test schema
    try:
        uc = UnityCatalog
        uc.query(f"CREATE SCHEMA IF NOT EXISTS {test_catalog}.{test_schema}")
        print(f"✓ Unity Catalog test schema ready: {test_catalog}.{test_schema}")
    except Exception as e:
        print(f"⚠ Could not create Unity Catalog test schema: {e}")
    
    print("="*60)
    print("Test environment ready!")
    print("="*60 + "\n")
    
    yield
    
    # Teardown note (we don't drop schemas to preserve data for debugging)
    print("\n" + "="*60)
    print("Test run complete!")
    print("Note: Test schemas preserved for debugging.")
    print(f"  - Unity Catalog: {test_catalog}.{test_schema}")
    print("="*60 + "\n")
