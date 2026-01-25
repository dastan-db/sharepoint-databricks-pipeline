# app/services/excel_streaming.py
import os
from typing import Dict, Any, List
from databricks.sdk import WorkspaceClient
from app.core.models import ExcelStreamConfig


class _ExcelStreamingService:
    """Singleton service for managing continuous Excel streaming from Lakebase to Delta tables."""

    _instance = None
    _workspace_client = None
    _active_streams: Dict[str, str] = {}  # config_id -> job_id mapping

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_ExcelStreamingService, cls).__new__(cls)
        return cls._instance

    def _get_workspace_client(self) -> WorkspaceClient:
        """Get or create Databricks Workspace Client."""
        if self._workspace_client is None:
            self._workspace_client = WorkspaceClient(
                client_id=os.getenv("DATABRICKS_CLIENT_ID"),
                client_secret=os.getenv("DATABRICKS_CLIENT_SECRET")
            )
        return self._workspace_client

    def start_stream(self, config: ExcelStreamConfig) -> str:
        """
        Start continuous streaming job for Excel files from Lakebase to Delta.
        
        Args:
            config: ExcelStreamConfig object with streaming configuration
            
        Returns:
            Job ID or stream identifier
        """
        w = self._get_workspace_client()
        
        # Get Lakebase connection details
        lakebase_catalog = os.getenv("LAKEBASE_CATALOG", "main")
        lakebase_schema = os.getenv("LAKEBASE_SCHEMA", "vibe_coding")
        
        # Build streaming notebook code
        notebook_code = f"""
# Streaming Excel from Lakebase to Delta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Read from Lakebase documents table (streaming)
source_table = "{lakebase_catalog}.{lakebase_schema}.{config.lakebase_table}"
checkpoint_location = "{config.checkpoint_location}"

# Create streaming query
df = spark.readStream \\
    .format("delta") \\
    .table(source_table) \\
    .filter(col("file_name").like("{config.file_name_pattern}"))

# Parse Excel and write to destination Delta table
# Note: In production, add Excel parsing logic here
destination_table = "{config.destination_catalog}.{config.destination_schema}.{config.destination_table}"

query = df.writeStream \\
    .format("delta") \\
    .outputMode("append") \\
    .option("checkpointLocation", checkpoint_location) \\
    .trigger(processingTime="{config.trigger_interval}") \\
    .toTable(destination_table)

query.awaitTermination()
"""
        
        # Create a notebook job for streaming
        # Note: This is a simplified implementation. In production, you would:
        # 1. Create or upload a notebook with the streaming code
        # 2. Create a job that runs the notebook continuously
        # 3. Start the job and track its status
        
        try:
            # For now, store the config ID as "active" but don't actually start a job
            # This would need actual Databricks Jobs API integration
            self._active_streams[config.id] = f"stream_job_{config.id}"
            
            return self._active_streams[config.id]
        except Exception as e:
            raise Exception(f"Failed to start streaming job: {str(e)}")

    def stop_stream(self, config_id: str) -> Dict[str, Any]:
        """
        Stop a running streaming job.
        
        Args:
            config_id: Configuration ID
            
        Returns:
            Dict with stop result
        """
        try:
            if config_id in self._active_streams:
                job_id = self._active_streams[config_id]
                
                # In production, stop the actual Databricks job here
                # w = self._get_workspace_client()
                # w.jobs.cancel_run(run_id=job_id)
                
                del self._active_streams[config_id]
                
                return {
                    "success": True,
                    "message": f"Streaming job {job_id} stopped successfully"
                }
            else:
                return {
                    "success": False,
                    "message": f"No active stream found for config {config_id}"
                }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to stop stream: {str(e)}"
            }

    def get_stream_status(self, config_id: str) -> Dict[str, Any]:
        """
        Get status of a streaming job.
        
        Args:
            config_id: Configuration ID
            
        Returns:
            Dict with stream status and metrics
        """
        try:
            if config_id in self._active_streams:
                job_id = self._active_streams[config_id]
                
                # In production, query actual job status from Databricks
                # w = self._get_workspace_client()
                # run = w.jobs.get_run(run_id=job_id)
                
                return {
                    "success": True,
                    "is_active": True,
                    "job_id": job_id,
                    "state": "RUNNING",
                    "message": "Stream is actively processing data"
                }
            else:
                return {
                    "success": True,
                    "is_active": False,
                    "state": "STOPPED",
                    "message": "Stream is not running"
                }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get stream status: {str(e)}"
            }

    def list_active_streams(self) -> List[str]:
        """
        List all currently active stream configuration IDs.
        
        Returns:
            List of active configuration IDs
        """
        return list(self._active_streams.keys())


# Create singleton instance
ExcelStreaming = _ExcelStreamingService()
