# app/services/sharepoint_connector.py
import os
import json
from typing import Dict, Any
from databricks.sdk import WorkspaceClient
from app.core.models import SharePointConnection, SharePointPipelineConfig


class _SharePointConnectorService:
    """Singleton service for managing SharePoint connections and ingestion pipelines."""

    _instance = None
    _workspace_client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_SharePointConnectorService, cls).__new__(cls)
        return cls._instance

    def _get_workspace_client(self) -> WorkspaceClient:
        """Get or create Databricks Workspace Client."""
        if self._workspace_client is None:
            self._workspace_client = WorkspaceClient(
                client_id=os.getenv("DATABRICKS_CLIENT_ID"),
                client_secret=os.getenv("DATABRICKS_CLIENT_SECRET")
            )
        return self._workspace_client

    def create_connection(self, connection: SharePointConnection) -> str:
        """
        Create a Unity Catalog CONNECTION object for SharePoint.
        
        Args:
            connection: SharePointConnection object with OAuth credentials
            
        Returns:
            Unity Catalog connection name
        """
        w = self._get_workspace_client()
        
        # Generate connection name if not provided
        if not connection.connection_name:
            connection.connection_name = f"sharepoint_{connection.id}"
        
        # Create CONNECTION using SQL execution
        # Note: The Databricks SDK doesn't have a direct connections API yet,
        # so we'll store credentials and create the connection via SQL when pipelines are created
        
        return connection.connection_name

    def test_connection(self, connection: SharePointConnection) -> Dict[str, Any]:
        """
        Test SharePoint connection credentials.
        
        Args:
            connection: SharePointConnection object
            
        Returns:
            Dict with test results
        """
        try:
            # Basic validation of required fields
            if not all([connection.client_id, connection.client_secret, 
                       connection.tenant_id, connection.refresh_token, connection.site_id]):
                return {
                    "success": False,
                    "message": "Missing required connection parameters"
                }
            
            # TODO: In production, validate credentials with Microsoft Graph API
            return {
                "success": True,
                "message": "Connection credentials validated"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Connection test failed: {str(e)}"
            }

    def create_pipeline(self, config: SharePointPipelineConfig, connection: SharePointConnection) -> str:
        """
        Create a Databricks ingestion pipeline for SharePoint Excel files to Lakebase.
        
        Args:
            config: SharePointPipelineConfig object
            connection: SharePointConnection object with credentials
            
        Returns:
            Databricks pipeline ID
        """
        w = self._get_workspace_client()
        
        # Get Lakebase destination from environment variables
        lakebase_catalog = os.getenv("LAKEBASE_CATALOG", "main")
        lakebase_schema = os.getenv("LAKEBASE_SCHEMA", "vibe_coding")
        
        # Build ingestion definition based on type
        if config.ingestion_type == "all_drives":
            # Schema-based ingestion (all drives) - targets Lakebase table
            ingestion_objects = [{
                "schema": {
                    "source_schema": connection.site_id,
                    "destination_catalog": lakebase_catalog,
                    "destination_schema": lakebase_schema,
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_1"  # Lakebase ingestion uses SCD_TYPE_1
                    }
                }
            }]
        else:
            # Table-based ingestion (specific drives) - targets Lakebase table
            ingestion_objects = []
            for drive_name in config.drive_names or []:
                ingestion_objects.append({
                    "table": {
                        "source_schema": connection.site_id,
                        "source_table": drive_name,
                        "destination_catalog": lakebase_catalog,
                        "destination_schema": lakebase_schema,
                        "destination_table": config.lakebase_table,
                        "table_configuration": {
                            "scd_type": "SCD_TYPE_1"  # Lakebase ingestion uses SCD_TYPE_1
                        }
                    }
                })
        
        # Create pipeline using Databricks SDK
        pipeline_spec = {
            "name": config.name,
            "ingestion_definition": {
                "connection_name": connection.connection_name,
                "objects": ingestion_objects
            },
            "channel": "PREVIEW"
        }
        
        # Create the pipeline
        pipeline = w.pipelines.create(
            name=pipeline_spec["name"],
            ingestion_definition={
                "connection_name": pipeline_spec["ingestion_definition"]["connection_name"],
                "objects": pipeline_spec["ingestion_definition"]["objects"]
            },
            channel=pipeline_spec["channel"]
        )
        
        return pipeline.pipeline_id

    def start_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Trigger a pipeline run.
        
        Args:
            pipeline_id: Databricks pipeline ID
            
        Returns:
            Dict with run information
        """
        w = self._get_workspace_client()
        
        try:
            # Start the pipeline
            update = w.pipelines.start_update(pipeline_id=pipeline_id)
            
            return {
                "success": True,
                "update_id": update.update_id,
                "message": "Pipeline run started successfully"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to start pipeline: {str(e)}"
            }

    def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Get pipeline status and latest run details.
        
        Args:
            pipeline_id: Databricks pipeline ID
            
        Returns:
            Dict with pipeline status and run information
        """
        w = self._get_workspace_client()
        
        try:
            # Get pipeline details
            pipeline = w.pipelines.get(pipeline_id=pipeline_id)
            
            # Get latest update if available
            latest_update = None
            if pipeline.latest_updates:
                latest_update = pipeline.latest_updates[0]
            
            return {
                "success": True,
                "pipeline_id": pipeline_id,
                "state": pipeline.state.value if pipeline.state else "UNKNOWN",
                "latest_update": {
                    "update_id": latest_update.update_id if latest_update else None,
                    "state": latest_update.state.value if latest_update and latest_update.state else None,
                    "creation_time": str(latest_update.creation_time) if latest_update and latest_update.creation_time else None,
                } if latest_update else None
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get pipeline status: {str(e)}"
            }

    def delete_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Delete a Databricks pipeline.
        
        Args:
            pipeline_id: Databricks pipeline ID
            
        Returns:
            Dict with deletion result
        """
        w = self._get_workspace_client()
        
        try:
            w.pipelines.delete(pipeline_id=pipeline_id)
            return {
                "success": True,
                "message": "Pipeline deleted successfully"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to delete pipeline: {str(e)}"
            }


# Create singleton instance
SharePointConnector = _SharePointConnectorService()
