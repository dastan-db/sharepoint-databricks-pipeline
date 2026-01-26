# app/services/unity_catalog.py
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import os
from typing import List, Dict, Any
import threading


class _UnityCatalog:
    """Singleton service for querying Unity Catalog tables via SQL warehouse."""
    
    _instance = None
    _client = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_UnityCatalog, cls).__new__(cls)
        return cls._instance
    
    def _get_client(self):
        """Get or create WorkspaceClient."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = WorkspaceClient(
                        host=os.getenv("DATABRICKS_HOST"),
                        token=os.getenv("DATABRICKS_TOKEN")
                    )
        return self._client
    
    def query(self, sql: str, warehouse_id: str = None) -> List[Dict[str, Any]]:
        """
        Execute SQL query against Unity Catalog.
        
        Args:
            sql: The SQL query to execute
            warehouse_id: Optional warehouse ID (uses env var if not provided)
            
        Returns:
            List of dictionaries with query results
        """
        if warehouse_id is None:
            warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        
        if not warehouse_id:
            raise ValueError("DATABRICKS_WAREHOUSE_ID not set in environment")
        
        client = self._get_client()
        
        # Execute statement
        statement = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="50s"
        )
        
        # Wait for completion and get results
        if statement.status.state == StatementState.SUCCEEDED:
            # Parse results into list of dicts
            if statement.result and statement.result.data_array:
                columns = [col.name for col in statement.manifest.schema.columns]
                results = []
                for row in statement.result.data_array:
                    results.append(dict(zip(columns, row)))
                return results
            return []
        elif statement.status.state == StatementState.FAILED:
            error_msg = statement.status.error.message if statement.status.error else "Unknown error"
            raise Exception(f"Query failed: {error_msg}")
        else:
            raise Exception(f"Query in unexpected state: {statement.status.state}")


# Create singleton instance
UnityCatalog = _UnityCatalog()
