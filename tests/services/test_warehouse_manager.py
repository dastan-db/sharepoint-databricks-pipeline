"""
Test WarehouseManager service (services/warehouse_manager.py).
Tests intelligent warehouse selection with dev/prod support.
"""
import pytest
import os
from app.services.warehouse_manager import WarehouseManager


def test_warehouse_manager_is_singleton():
    """Test that WarehouseManager is a singleton."""
    from app.services.warehouse_manager import _WarehouseManager
    instance1 = _WarehouseManager()
    instance2 = _WarehouseManager()
    assert instance1 is instance2


def test_warehouse_manager_get_warehouse_id():
    """Test WarehouseManager.get_warehouse_id() returns warehouse ID."""
    warehouse_id = WarehouseManager.get_warehouse_id()
    
    # Should return either from env var or auto-selection
    assert warehouse_id is not None
    assert isinstance(warehouse_id, str)
    assert len(warehouse_id) > 0


def test_warehouse_manager_respects_env_var():
    """Test WarehouseManager prioritizes DATABRICKS_WAREHOUSE_ID env var."""
    env_warehouse = os.getenv("DATABRICKS_WAREHOUSE_ID")
    
    if env_warehouse:
        warehouse_id = WarehouseManager.get_warehouse_id()
        assert warehouse_id == env_warehouse


def test_warehouse_manager_auto_select():
    """Test WarehouseManager can auto-select warehouse via MCP."""
    # Force auto-selection (ignore env var)
    WarehouseManager.clear_cache()
    warehouse_id = WarehouseManager.get_warehouse_id(force_auto_select=True)
    
    # Should get a warehouse ID from MCP
    assert warehouse_id is not None
    assert isinstance(warehouse_id, str)


def test_warehouse_manager_caches_result():
    """Test WarehouseManager caches auto-selected warehouse."""
    WarehouseManager.clear_cache()
    
    # First call
    warehouse_id_1 = WarehouseManager.get_warehouse_id(force_auto_select=True)
    
    # Second call should return cached value
    warehouse_id_2 = WarehouseManager.get_warehouse_id(force_auto_select=True)
    
    assert warehouse_id_1 == warehouse_id_2
    assert WarehouseManager._cached_warehouse_id is not None


def test_warehouse_manager_clear_cache():
    """Test WarehouseManager.clear_cache() clears cached warehouse."""
    # Get a warehouse (will be cached)
    WarehouseManager.get_warehouse_id(force_auto_select=True)
    assert WarehouseManager._cached_warehouse_id is not None
    
    # Clear cache
    WarehouseManager.clear_cache()
    assert WarehouseManager._cached_warehouse_id is None
