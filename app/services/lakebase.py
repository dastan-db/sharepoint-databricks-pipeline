# app/services/lakebase.py
import os
import uuid
from datetime import datetime, timedelta
from typing import List
import psycopg2
from databricks.sdk import WorkspaceClient
import threading


class _Lakebase:
    """Singleton service for Databricks Lakebase connections via Postgres protocol."""

    _instance = None
    _connection = None
    _connection_time = None
    _lock = threading.Lock()  # Thread-safe connection creation

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_Lakebase, cls).__new__(cls)
        return cls._instance

    def _should_refresh_connection(self) -> bool:
        """Check if connection should be refreshed (>59 minutes old)."""
        if self._connection_time is None:
            return True
        elapsed = datetime.now() - self._connection_time
        return elapsed > timedelta(minutes=59)

    def _create_connection(self):
        """Create a new Lakebase connection."""
        # Use token-based auth (PAT) instead of OAuth
        w = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        )
        instance_name = os.getenv("LAKEBASE_INSTANCE_NAME")
        db_name = os.getenv("LAKEBASE_DB_NAME")
        # Use email as database user when using PAT authentication
        db_user = os.getenv("MY_EMAIL")
        
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[instance_name]
        )
        instance = w.database.get_database_instance(name=instance_name)
        
        self._connection = psycopg2.connect(
            host=instance.read_write_dns,
            dbname=db_name,
            user=db_user,
            password=cred.token,
            sslmode="require",
        )
        self._connection_time = datetime.now()

    def _ensure_connection(self):
        """Ensure we have a valid connection (thread-safe)."""
        # Quick check without lock (optimization)
        if self._connection is not None and not self._should_refresh_connection():
            return
        
        # Acquire lock for connection creation
        with self._lock:
            # Double-check after acquiring lock (another thread may have created it)
            if self._connection is not None and not self._should_refresh_connection():
                return
                
            if self._connection is not None:
                try:
                    self._connection.close()
                except:
                    pass
            self._create_connection()

    def query(self, sql: str) -> List:
        """
        Execute a SQL query and return results.
        
        Args:
            sql: The SQL query to execute
            
        Returns:
            List of row tuples
        """
        self._ensure_connection()
        
        with self._connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            self._connection.commit()
            return rows


# Create singleton instance
Lakebase = _Lakebase()
