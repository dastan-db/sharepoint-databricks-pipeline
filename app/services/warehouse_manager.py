# app/services/warehouse_manager.py
"""
WarehouseManager Service - Intelligent warehouse selection with dev/prod support.
Uses MCP get_best_warehouse for automatic selection in development environments.
Falls back to DATABRICKS_WAREHOUSE_ID for explicit production configuration.
"""
import os
from typing import Optional
import threading


class _WarehouseManager:
    """Singleton service for warehouse ID resolution with automatic selection."""
    
    _instance = None
    _cached_warehouse_id: Optional[str] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_WarehouseManager, cls).__new__(cls)
        return cls._instance
    
    def get_warehouse_id(self, force_auto_select: bool = False) -> Optional[str]:
        """
        Get warehouse ID based on environment configuration.
        
        Priority:
        1. Explicit DATABRICKS_WAREHOUSE_ID env var (production)
        2. Auto-select via MCP get_best_warehouse (development)
        
        Args:
            force_auto_select: Force auto-selection even if env var is set
            
        Returns:
            Warehouse ID string, or None if no warehouse available
        """
        # Check environment variable first (production/explicit config)
        if not force_auto_select:
            env_warehouse = os.getenv("DATABRICKS_WAREHOUSE_ID")
            if env_warehouse:
                return env_warehouse
        
        # Auto-select best warehouse (development/dynamic environments)
        if self._cached_warehouse_id is None:
            with self._lock:
                # Double-check after acquiring lock
                if self._cached_warehouse_id is None:
                    try:
                        from app.core.mcp_client import call_mcp_tool
                        
                        result = call_mcp_tool(
                            server="project-0-fe-vibe-app-databricks",
                            tool_name="get_best_warehouse",
                            arguments={}
                        )
                        self._cached_warehouse_id = result.get("result")
                    except Exception as e:
                        print(f"Warning: Failed to auto-select warehouse: {e}")
                        return None
        
        return self._cached_warehouse_id
    
    def clear_cache(self):
        """Clear cached warehouse ID (useful for testing or warehouse changes)"""
        with self._lock:
            self._cached_warehouse_id = None


# Create singleton instance
WarehouseManager = _WarehouseManager()
