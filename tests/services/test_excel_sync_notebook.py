"""
Test ExcelSyncNotebook service (services/excel_sync_notebook.py).
Tests notebook code generation for Excel sync tasks.
"""
import pytest
from app.services.excel_sync_notebook import ExcelSyncNotebook


def test_excel_sync_notebook_is_singleton():
    """Test that ExcelSyncNotebook is a singleton."""
    from app.services.excel_sync_notebook import _ExcelSyncNotebookService
    instance1 = _ExcelSyncNotebookService()
    instance2 = _ExcelSyncNotebookService()
    assert instance1 is instance2


def test_generate_sync_notebook_basic():
    """Test generate_sync_notebook() creates valid notebook code."""
    notebook_code = ExcelSyncNotebook.generate_sync_notebook(
        document_table="main.default.documents",
        tracked_file_path="test_file.xlsx",
        target_table="main.default.test_target",
        header_row=0
    )
    
    assert isinstance(notebook_code, str)
    assert len(notebook_code) > 0
    
    # Should contain Databricks notebook markers
    assert "# Databricks notebook source" in notebook_code
    
    # Should reference the tables
    assert "main.default.documents" in notebook_code
    assert "main.default.test_target" in notebook_code
    assert "test_file.xlsx" in notebook_code


def test_generate_sync_notebook_with_selected_columns():
    """Test generate_sync_notebook() with selected columns."""
    selected_columns = ["col1", "col2", "col3"]
    
    notebook_code = ExcelSyncNotebook.generate_sync_notebook(
        document_table="main.default.documents",
        tracked_file_path="test_file.xlsx",
        target_table="main.default.test_target",
        header_row=0,
        selected_columns=selected_columns
    )
    
    assert isinstance(notebook_code, str)
    # Should include column selection logic
    assert "selected_columns" in notebook_code
    assert "col1" in notebook_code


def test_generate_sync_notebook_with_custom_header_row():
    """Test generate_sync_notebook() with custom header row."""
    notebook_code = ExcelSyncNotebook.generate_sync_notebook(
        document_table="main.default.documents",
        tracked_file_path="test_file.xlsx",
        target_table="main.default.test_target",
        header_row=2  # Third row is header
    )
    
    assert isinstance(notebook_code, str)
    # Should use header_row=2 (check variable assignment, not pandas parameter)
    assert "header_row = 2" in notebook_code


def test_generate_sync_notebook_no_selected_columns():
    """Test generate_sync_notebook() without column selection (all columns)."""
    notebook_code = ExcelSyncNotebook.generate_sync_notebook(
        document_table="main.default.documents",
        tracked_file_path="test_file.xlsx",
        target_table="main.default.test_target",
        header_row=0,
        selected_columns=None
    )
    
    assert isinstance(notebook_code, str)
    # Should handle all columns (no filtering)


def test_get_notebook_path():
    """Test get_notebook_path() generates correct path."""
    connection_id = "test_connection_123"
    
    notebook_path = ExcelSyncNotebook.get_notebook_path(connection_id)
    
    assert isinstance(notebook_path, str)
    assert connection_id in notebook_path
    # Should be in Users folder or Shared folder
    assert "/Users/" in notebook_path or "/Shared/" in notebook_path


def test_generate_sync_notebook_contains_cdc_logic():
    """Test generate_sync_notebook() includes CDC (change detection) logic."""
    notebook_code = ExcelSyncNotebook.generate_sync_notebook(
        document_table="main.default.documents",
        tracked_file_path="test_file.xlsx",
        target_table="main.default.test_target",
        header_row=0
    )
    
    # Should include file modification check logic
    assert "file_id" in notebook_code or "tracked_file_path" in notebook_code
    # Should include merge/upsert logic or timestamp comparison
    assert "SELECT" in notebook_code  # Query for file metadata


def test_generate_sync_notebook_handles_special_characters():
    """Test generate_sync_notebook() handles special characters in names."""
    notebook_code = ExcelSyncNotebook.generate_sync_notebook(
        document_table="main.test_schema.test_docs",
        tracked_file_path="file-with-dashes_and_underscores.xlsx",
        target_table="main.test_schema.target_table_2024",
        header_row=0
    )
    
    assert isinstance(notebook_code, str)
    assert "file-with-dashes_and_underscores.xlsx" in notebook_code


def test_generated_notebook_is_valid_python():
    """Test that generated notebook contains valid Python code."""
    notebook_code = ExcelSyncNotebook.generate_sync_notebook(
        document_table="main.default.documents",
        tracked_file_path="test.xlsx",
        target_table="main.default.target",
        header_row=0
    )
    
    # Remove Databricks-specific syntax for validation
    lines = notebook_code.split("\n")
    python_lines = []
    for line in lines:
        # Skip Databricks magic commands and %pip commands
        if line.strip().startswith("# MAGIC") or line.strip().startswith("%"):
            continue
        python_lines.append(line)
    
    python_code = "\n".join(python_lines)
    
    # Should be syntactically valid Python (no syntax errors)
    try:
        compile(python_code, "<string>", "exec")
    except SyntaxError as e:
        pytest.fail(f"Generated notebook has invalid Python syntax: {e}")
