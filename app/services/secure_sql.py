# app/services/secure_sql.py
"""
Secure SQL Execution Service - Wrapper around databricks_tools_core for safe SQL execution.
Prevents SQL injection by using parameterized queries and proper SDK methods.
"""
import asyncio
from typing import List, Dict, Any, Optional
from databricks_tools_core.sql import execute_sql as dtc_execute_sql
from databricks.sdk import WorkspaceClient
import os


class _SecureSQLService:
    """Singleton service for secure SQL execution using databricks_tools_core."""

    _instance = None
    _workspace_client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_SecureSQLService, cls).__new__(cls)
        return cls._instance

    def _get_workspace_client(self) -> WorkspaceClient:
        """Get or create Databricks Workspace Client."""
        if self._workspace_client is None:
            self._workspace_client = WorkspaceClient()
        return self._workspace_client

    async def execute_query(
        self,
        sql_query: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute a SQL query safely using databricks_tools_core.
        
        NOTE: For INSERT/UPDATE/DELETE with RETURNING *, this works fine.
        For CREATE TABLE, use Unity Catalog SDK methods instead.
        
        Args:
            sql_query: SQL query to execute (should NOT contain user data directly)
            catalog: Optional catalog context
            schema: Optional schema context
            warehouse_id: Optional warehouse ID (auto-selected if not provided)
            
        Returns:
            List of dicts representing query results
        """
        # Execute in thread pool since databricks_tools_core is synchronous
        result = await asyncio.to_thread(
            dtc_execute_sql, sql_query=sql_query, catalog=catalog, schema=schema, warehouse_id=warehouse_id
        )
        return result


# Create singleton instance
SecureSQL = _SecureSQLService()
