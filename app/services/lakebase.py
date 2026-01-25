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
        # region agent log
        import json, time
        log_start = time.time()
        with open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log', 'a') as f:
            f.write(json.dumps({'location':'lakebase.py:29','message':'Starting connection creation','data':{'connection_exists':self._connection is not None},'timestamp':int(time.time()*1000),'sessionId':'debug-session','hypothesisId':'H2,H4'}) + '\n')
        # endregion
        
        w = WorkspaceClient(
            client_id=os.getenv("DATABRICKS_CLIENT_ID"),
            client_secret=os.getenv("DATABRICKS_CLIENT_SECRET")
        )
        instance_name = os.getenv("LAKEBASE_INSTANCE_NAME")
        db_name = os.getenv("LAKEBASE_DB_NAME")
        db_user = "2025_vibe_coding"

        # region agent log
        with open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log', 'a') as f:
            f.write(json.dumps({'location':'lakebase.py:39','message':'About to generate credentials','data':{'instance_name':instance_name},'timestamp':int(time.time()*1000),'sessionId':'debug-session','hypothesisId':'H4'}) + '\n')
        # endregion
        
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[instance_name]
        )
        instance = w.database.get_database_instance(name=instance_name)

        # region agent log
        cred_time = time.time() - log_start
        with open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log', 'a') as f:
            f.write(json.dumps({'location':'lakebase.py:44','message':'Credentials generated, about to connect','data':{'credential_gen_time_sec':cred_time,'has_token':bool(cred.token)},'timestamp':int(time.time()*1000),'sessionId':'debug-session','hypothesisId':'H4'}) + '\n')
        # endregion
        
        self._connection = psycopg2.connect(
            host=instance.read_write_dns,
            dbname=db_name,
            user=db_user,
            password=cred.token,
            sslmode="require",
        )
        self._connection_time = datetime.now()
        
        # region agent log
        total_time = time.time() - log_start
        with open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log', 'a') as f:
            f.write(json.dumps({'location':'lakebase.py:51','message':'Connection created successfully','data':{'total_time_sec':total_time,'connection_established':True},'timestamp':int(time.time()*1000),'sessionId':'debug-session','hypothesisId':'H2,H4'}) + '\n')
        # endregion

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
        # region agent log
        import json, time
        query_start = time.time()
        with open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log', 'a') as f:
            f.write(json.dumps({'location':'lakebase.py:63','message':'Query called','data':{'sql_preview':sql[:100],'connection_exists':self._connection is not None},'timestamp':int(time.time()*1000),'sessionId':'debug-session','hypothesisId':'H2,H3'}) + '\n')
        # endregion
        
        self._ensure_connection()
        
        # region agent log
        ensure_time = time.time() - query_start
        with open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log', 'a') as f:
            f.write(json.dumps({'location':'lakebase.py:73','message':'Connection ensured','data':{'ensure_time_sec':ensure_time},'timestamp':int(time.time()*1000),'sessionId':'debug-session','hypothesisId':'H2'}) + '\n')
        # endregion
        
        with self._connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            self._connection.commit()
            
            # region agent log
            total_time = time.time() - query_start
            with open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log', 'a') as f:
                f.write(json.dumps({'location':'lakebase.py:79','message':'Query completed','data':{'total_time_sec':total_time,'row_count':len(rows)},'timestamp':int(time.time()*1000),'sessionId':'debug-session','hypothesisId':'H1,H2'}) + '\n')
            # endregion
            
            return rows


# Create singleton instance
Lakebase = _Lakebase()
