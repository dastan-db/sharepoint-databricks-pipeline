# app/services/job_orchestrator.py
"""
JobOrchestrator Service - Native Databricks Jobs orchestration.
Replaces custom threading/sleep loops with Databricks Jobs API.
Uses databricks_tools_core.jobs for job management.
"""
import os
import asyncio
from typing import Dict, Any, Optional
from databricks_tools_core.jobs import (
    create_job,
    find_job_by_name,
    get_job,
    update_job,
    delete_job,
    run_job_now,
    get_run,
    cancel_run,
    wait_for_run,
)
from databricks.sdk.service.jobs import Task, NotebookTask, Source
from app.core.models import ExcelStreamConfig


class _JobOrchestratorService:
    """
    Singleton service for managing Databricks Jobs orchestration.
    Replaces custom streaming loops with native Databricks Jobs.
    """

    _instance = None
    _active_jobs: Dict[str, str] = {}  # config_id -> job_id mapping
    _active_runs: Dict[str, str] = {}  # config_id -> run_id mapping

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_JobOrchestratorService, cls).__new__(cls)
        return cls._instance

    async def _create_or_get_streaming_job(self, config: ExcelStreamConfig) -> str:
        """
        Create or get existing Databricks Job for Excel streaming.
        
        Args:
            config: ExcelStreamConfig with streaming configuration
            
        Returns:
            Job ID
        """
        job_name = f"excel_stream_{config.id}"
        
        # Check if job already exists
        def check_existing_job():
            return find_job_by_name(job_name)
        
        existing_job_id = await asyncio.to_thread(check_existing_job)
        if existing_job_id:
            return existing_job_id
        
        # Create inline notebook code for streaming
        notebook_code = self._generate_streaming_notebook(config)
        
        # Create a temporary notebook in workspace
        # Note: In production, you'd upload this to a proper notebook location
        notebook_path = f"/Workspace/Shared/excel_streaming/{config.id}_stream"
        
        # Define job configuration using databricks_tools_core
        def create_streaming_job():
            tasks = [
                {
                    "task_key": "excel_stream_task",
                    "description": f"Stream Excel files from {config.lakebase_table} to {config.destination_table}",
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "source": "WORKSPACE",
                    },
                    # Serverless by default - no cluster configuration needed
                }
            ]
            
            job_config = create_job(
                name=job_name,
                tasks=tasks,
                description=f"Excel streaming job for config: {config.name}",
            )
            return job_config["job_id"]
        
        job_id = await asyncio.to_thread(create_streaming_job)
        return job_id

    def _generate_streaming_notebook(self, config: ExcelStreamConfig) -> str:
        """
        Generate notebook code for Excel streaming.
        
        Args:
            config: ExcelStreamConfig
            
        Returns:
            Python notebook code as string
        """
        lakebase_catalog = os.getenv("LAKEBASE_CATALOG", "main")
        lakebase_schema = os.getenv("LAKEBASE_SCHEMA", "vibe_coding")
        
        code = f'''
# Excel Streaming Notebook - Auto-generated
# Config ID: {config.id}
# Config Name: {config.name}

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Source: Lakebase documents table
source_table = "{lakebase_catalog}.{lakebase_schema}.{config.lakebase_table}"
checkpoint_location = "{config.checkpoint_location}"

# Destination: Delta table
destination_table = "{config.destination_catalog}.{config.destination_schema}.{config.destination_table}"

print(f"Starting streaming from {{source_table}} to {{destination_table}}")
print(f"File pattern: {config.file_name_pattern}")
print(f"Checkpoint: {{checkpoint_location}}")
print(f"Trigger interval: {config.trigger_interval}")

# Create streaming query with Excel filtering
df = spark.readStream \\
    .format("delta") \\
    .table(source_table) \\
    .filter(col("file_name").like("{config.file_name_pattern}"))

# Write to destination Delta table
query = df.writeStream \\
    .format("delta") \\
    .outputMode("append") \\
    .option("checkpointLocation", checkpoint_location) \\
    .trigger(processingTime="{config.trigger_interval}") \\
    .toTable(destination_table)

print(f"Streaming query started. Query ID: {{query.id}}")
print("Streaming will continue until job is cancelled...")

# Keep the stream running
query.awaitTermination()
'''
        return code

    async def start_streaming_job(self, config: ExcelStreamConfig) -> Dict[str, Any]:
        """
        Start a Databricks Job for Excel streaming.
        
        Args:
            config: ExcelStreamConfig
            
        Returns:
            Dict with job_id, run_id, and status
        """
        try:
            # Get or create job
            job_id = await self._create_or_get_streaming_job(config)
            self._active_jobs[config.id] = job_id
            
            # Start the job run
            def start_run():
                return run_job_now(job_id=int(job_id))
            
            run_id = await asyncio.to_thread(start_run)
            self._active_runs[config.id] = str(run_id)
            
            return {
                "success": True,
                "job_id": job_id,
                "run_id": run_id,
                "message": f"Streaming job started for {config.name}",
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to start streaming job: {str(e)}",
            }

    async def stop_streaming_job(self, config_id: str) -> Dict[str, Any]:
        """
        Stop a running Databricks streaming job.
        
        Args:
            config_id: Configuration ID
            
        Returns:
            Dict with stop status
        """
        try:
            if config_id not in self._active_runs:
                return {
                    "success": False,
                    "message": f"No active run found for config {config_id}",
                }
            
            run_id = self._active_runs[config_id]
            
            # Cancel the job run
            def cancel_job_run():
                cancel_run(run_id=int(run_id))
            
            await asyncio.to_thread(cancel_job_run)
            
            # Clean up tracking
            del self._active_runs[config_id]
            
            return {
                "success": True,
                "message": f"Streaming job {run_id} stopped successfully",
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to stop streaming job: {str(e)}",
            }

    async def get_streaming_job_status(self, config_id: str) -> Dict[str, Any]:
        """
        Get status of a streaming job.
        
        Args:
            config_id: Configuration ID
            
        Returns:
            Dict with job status and metrics
        """
        try:
            if config_id not in self._active_runs:
                return {
                    "success": True,
                    "is_active": False,
                    "state": "STOPPED",
                    "message": "Streaming job is not running",
                }
            
            run_id = self._active_runs[config_id]
            job_id = self._active_jobs.get(config_id)
            
            # Get run status
            def get_run_status():
                return get_run(run_id=int(run_id))
            
            run_info = await asyncio.to_thread(get_run_status)
            
            lifecycle_state = run_info.get("state", {}).get("life_cycle_state", "UNKNOWN")
            result_state = run_info.get("state", {}).get("state_message", "")
            
            is_active = lifecycle_state in ["PENDING", "RUNNING", "TERMINATING"]
            
            return {
                "success": True,
                "is_active": is_active,
                "job_id": job_id,
                "run_id": run_id,
                "lifecycle_state": lifecycle_state,
                "result_state": result_state,
                "state": lifecycle_state,
                "message": f"Streaming job is {lifecycle_state}",
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get streaming job status: {str(e)}",
            }

    async def list_active_jobs(self) -> Dict[str, str]:
        """
        List all currently active streaming jobs.
        
        Returns:
            Dict mapping config_id to job_id
        """
        return dict(self._active_jobs)


# Create singleton instance
JobOrchestrator = _JobOrchestratorService()
