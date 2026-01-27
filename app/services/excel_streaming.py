# app/services/excel_streaming.py
"""
Excel Streaming Service - Refactored to use Databricks Jobs orchestration.
Now delegates to JobOrchestrator instead of managing streams with custom loops.
"""
from typing import Dict, Any, List
from app.core.models import ExcelStreamConfig
from app.services.job_orchestrator import JobOrchestrator


class _ExcelStreamingService:
    """
    Singleton service for managing Excel streaming orchestration.
    Delegates to JobOrchestrator for native Databricks Jobs management.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_ExcelStreamingService, cls).__new__(cls)
        return cls._instance

    def start_stream(self, config: ExcelStreamConfig) -> str:
        """
        Start streaming job for Excel files using Databricks Jobs.
        
        Args:
            config: ExcelStreamConfig object with streaming configuration
            
        Returns:
            Job ID
            
        Note: This is now a wrapper around JobOrchestrator.
        In a fully async context, this should be async, but maintaining
        sync interface for backward compatibility with current routes.
        """
        import asyncio
        
        # Run the async job orchestrator in a new event loop if needed
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an async context, create a task
                # This is a simplification - in production you'd handle this better
                raise RuntimeError(
                    "start_stream called from async context. "
                    "Use JobOrchestrator.start_streaming_job directly for async calls."
                )
        except RuntimeError:
            # No event loop, create one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        result = loop.run_until_complete(JobOrchestrator.start_streaming_job(config))
        
        if not result.get("success"):
            raise Exception(result.get("message", "Failed to start streaming job"))
        
        return result.get("job_id", "")

    def stop_stream(self, config_id: str) -> Dict[str, Any]:
        """
        Stop a running streaming job.
        
        Args:
            config_id: Configuration ID
            
        Returns:
            Dict with stop result
        """
        import asyncio
        
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError(
                    "stop_stream called from async context. "
                    "Use JobOrchestrator.stop_streaming_job directly for async calls."
                )
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        return loop.run_until_complete(JobOrchestrator.stop_streaming_job(config_id))

    def get_stream_status(self, config_id: str) -> Dict[str, Any]:
        """
        Get status of a streaming job.
        
        Args:
            config_id: Configuration ID
            
        Returns:
            Dict with stream status and metrics
        """
        import asyncio
        
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError(
                    "get_stream_status called from async context. "
                    "Use JobOrchestrator.get_streaming_job_status directly for async calls."
                )
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        return loop.run_until_complete(JobOrchestrator.get_streaming_job_status(config_id))

    def list_active_streams(self) -> List[str]:
        """
        List all currently active stream configuration IDs.
        
        Returns:
            List of active configuration IDs
        """
        import asyncio
        
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError(
                    "list_active_streams called from async context. "
                    "Use JobOrchestrator.list_active_jobs directly for async calls."
                )
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        active_jobs = loop.run_until_complete(JobOrchestrator.list_active_jobs())
        return list(active_jobs.keys())


# Create singleton instance
ExcelStreaming = _ExcelStreamingService()
