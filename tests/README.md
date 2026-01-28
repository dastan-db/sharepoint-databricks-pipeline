# Test Suite Documentation

Comprehensive backward testing suite for the SharePoint to Databricks Data Pipeline application.

## Overview

This test suite follows a **backward testing approach** - starting from API endpoints and working down through services to utilities. This methodology helps identify:

- **Actively used code** - Code covered by tests is proven to be in use
- **Dead code** - Untested code may be unused and can be removed
- **Integration issues** - End-to-end tests verify complete workflows

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures and test configuration
├── test_main.py             # Application initialization tests
├── api/                     # API endpoint tests (21 endpoints)
│   ├── test_routes_runs.py
│   ├── test_routes_config.py
│   ├── test_routes_lakeflow.py
│   ├── test_routes_excel.py
│   ├── test_routes_catalog.py
│   └── test_routes_sharepoint.py
├── core/                    # Core module tests
│   ├── test_models.py
│   ├── test_pipeline.py
│   └── test_mcp_client.py
├── services/                # Service layer tests
│   ├── test_lakebase.py
│   ├── test_unity_catalog.py
│   ├── test_warehouse_manager.py
│   ├── test_schema_manager.py
│   ├── test_excel_sync_notebook.py
│   ├── test_excel_parser.py
│   ├── test_update_checker.py
│   └── test_data_quality.py
└── integration/             # End-to-end integration tests
    ├── test_end_to_end_sync.py
    └── test_lakeflow_pipeline.py
```

## Running Tests

### All Tests

```bash
# Run all tests with coverage
./run_tests.sh

# Or manually:
pytest tests/ --cov=app --cov-report=html --cov-report=term -v
```

### Specific Test Categories

```bash
# API endpoint tests only
pytest tests/api/ -v

# Service tests only
pytest tests/services/ -v

# Integration tests only
pytest tests/integration/ -v

# Specific test file
pytest tests/api/test_routes_config.py -v

# Specific test function
pytest tests/api/test_routes_config.py::test_create_config -v
```

### With Markers

```bash
# Run only integration tests
pytest -m integration -v

# Skip slow tests
pytest -m "not slow" -v
```

## Test Environment Setup

### Required Environment Variables

Create a `.env` file with:

```bash
# Databricks Connection
DATABRICKS_HOST=https://your-workspace.databricks.com
DATABRICKS_TOKEN=your-access-token

# Lakebase (PostgreSQL via Databricks)
LAKEBASE_INSTANCE_NAME=your-instance
LAKEBASE_DB_NAME=your-database
LAKEBASE_CATALOG=main
LAKEBASE_SCHEMA=vibe_coding
MY_EMAIL=your-email@company.com

# Unity Catalog
UC_CATALOG=main
SHAREPOINT_SCHEMA_PREFIX=sharepoint

# Optional: Explicit warehouse (auto-selects if not set)
DATABRICKS_WAREHOUSE_ID=your-warehouse-id

# Test Configuration (optional)
TEST_CATALOG=main
TEST_SCHEMA=test_vibe_app_test
```

### Dependencies

Install test dependencies:

```bash
pip install -r requirements.txt
```

This includes:
- `pytest` - Test framework
- `pytest-asyncio` - Async test support
- `pytest-cov` - Coverage reporting
- `httpx` - FastAPI TestClient

## Test Fixtures

### Global Fixtures (conftest.py)

- `test_client` - FastAPI TestClient for endpoint testing
- `test_catalog` / `test_schema` - Test environment configuration
- `workspace_client` - Databricks SDK client
- `lakebase_connection` - Lakebase singleton instance
- `unity_catalog_connection` - UnityCatalog singleton instance

### Data Fixtures

- `sample_excel_file` - Sample Excel file for testing
- `sample_sync_config` - Valid SyncConfig model
- `sample_lakeflow_config` - Valid LakeflowJobConfig model

### Cleanup Fixtures

- `cleanup_lakebase_tables` - Auto-cleanup for Lakebase tables
- `cleanup_unity_tables` - Auto-cleanup for Unity Catalog tables
- `cleanup_lakeflow_jobs` - Auto-cleanup for Databricks jobs/pipelines

## Test Coverage

### Current Coverage

Run `./run_tests.sh` to generate coverage report. Coverage reports are saved to:

- HTML: `coverage_reports/html/index.html`
- JSON: `coverage_reports/coverage.json`
- Analysis: `coverage_reports/dead_code_analysis.txt`

### Coverage Analysis

The test suite includes automated dead code detection:

```bash
python analyze_coverage.py
```

This identifies:
- **Completely untested files** (0% coverage)
- **Low coverage files** (<50% coverage)
- **Potentially unused modules** (no test files)
- **Untested code paths**

## Test Categories

### 1. API Endpoint Tests (`tests/api/`)

Test all 21 FastAPI endpoints working backwards from user requests.

**Coverage:**
- ✅ Config CRUD operations (4 endpoints)
- ✅ Sync execution (2 endpoints)
- ✅ Lakeflow pipeline management (9 endpoints)
- ✅ Excel parsing (3 endpoints)
- ✅ Catalog discovery (3 endpoints)
- ✅ SharePoint connections (4 endpoints)

### 2. Service Tests (`tests/services/`)

Test critical services used by endpoints.

**Coverage:**
- ✅ Lakebase (PostgreSQL connection, query execution)
- ✅ UnityCatalog (Unity Catalog queries via MCP)
- ✅ WarehouseManager (intelligent warehouse selection)
- ✅ ExcelSyncNotebook (notebook code generation)
- ✅ SchemaManager (database initialization)
- ✅ Excel parser (Excel to Delta conversion)
- ✅ Update checker (file modification detection)
- ✅ Data quality (validation checks)

### 3. Core Tests (`tests/core/`)

Test core modules and models.

**Coverage:**
- ✅ Pydantic models (SyncConfig, RunResult, LakeflowJobConfig)
- ✅ Pipeline orchestration
- ✅ MCP client (Databricks tool integration)

### 4. Integration Tests (`tests/integration/`)

End-to-end workflow tests.

**Coverage:**
- ⚠️ Complete sync workflow (requires full environment)
- ⚠️ Lakeflow pipeline workflow (requires SharePoint)
- ✅ Config CRUD integration
- ✅ Catalog discovery integration

**Note:** Many integration tests are skipped by default as they require:
- Documents table with Excel files
- Valid SharePoint connections
- Databricks pipeline resources

## Skipped Tests

Some tests are marked with `@pytest.mark.skip()` because they require:

1. **Full environment setup**
   - Documents table populated with Excel files
   - Target tables configured
   - Databricks pipelines deployed

2. **External dependencies**
   - Valid SharePoint OAuth credentials
   - Active SharePoint connections in Unity Catalog
   - Ingested SharePoint documents

3. **Long-running operations**
   - Tests that take >60 minutes (e.g., connection refresh)

To run skipped tests:

```bash
pytest tests/ --run-skipped
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: ./run_tests.sh
      - name: Upload coverage
        uses: codecov/codecov-action@v2
        with:
          files: coverage_reports/coverage.json
```

## Writing New Tests

### Test Structure Template

```python
def test_feature_name(test_client: TestClient, cleanup_fixture):
    """
    Test description explaining what is being tested.
    
    SKIPPED: Reason if test requires special setup.
    """
    # Arrange
    test_data = {...}
    
    # Act
    response = test_client.post("/endpoint", json=test_data)
    
    # Assert
    assert response.status_code == 200
    assert response.json()["key"] == "expected_value"
    
    # Cleanup (if needed)
    cleanup_fixture("resource_name")
```

### Best Practices

1. **Use descriptive test names** - `test_create_config_success` not `test_config`
2. **Follow Arrange-Act-Assert** - Separate setup, execution, and verification
3. **Use fixtures for cleanup** - Prevent test pollution
4. **Test error cases** - Not just happy paths
5. **Skip appropriately** - Mark tests that need special setup
6. **Add docstrings** - Explain what's being tested and why

## Troubleshooting

### Common Issues

1. **Connection errors**
   - Check `.env` file has correct credentials
   - Verify Databricks workspace is accessible
   - Check network/VPN connection

2. **Test database issues**
   - Ensure test schemas exist
   - Check permissions for test user
   - Verify cleanup fixtures are running

3. **Import errors**
   - Run `pip install -r requirements.txt`
   - Check Python path includes project root

4. **Slow tests**
   - Use `pytest -m "not slow"` to skip slow tests
   - Check for resource cleanup (connections not closed)

### Debug Mode

Run tests with verbose output and no capture:

```bash
pytest tests/api/test_routes_config.py -vv -s
```

## Contributing

When adding new features:

1. Write tests **before** or **alongside** implementation
2. Follow the backward testing approach (endpoints → services → utilities)
3. Add cleanup fixtures for new resources
4. Update this README if adding new test categories
5. Run full test suite before submitting PR

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [FastAPI Testing](https://fastapi.tiangolo.com/tutorial/testing/)
- [Databricks SDK Testing](https://databricks-sdk-py.readthedocs.io/)
