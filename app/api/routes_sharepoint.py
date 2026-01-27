# app/api/routes_sharepoint.py
"""
SharePoint Routes - Manage Unity Catalog SharePoint connections.
Provides endpoints to list, create, and manage SharePoint connections in Unity Catalog.
"""
from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType
import os

router = APIRouter()


class SharePointConnectionCreate(BaseModel):
    """Model for creating a new SharePoint connection."""
    id: str
    name: str
    client_id: str
    client_secret: str
    tenant_id: str
    refresh_token: str
    site_id: str = ""
    connection_name: str


def _get_workspace_client() -> WorkspaceClient:
    """Get Databricks Workspace Client."""
    return WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )


@router.get("/connections")
async def list_sharepoint_connections() -> List[Dict[str, Any]]:
    """
    List all SharePoint connections from Unity Catalog.
    
    Returns a list of SharePoint connections with their metadata.
    """
    try:
        w = _get_workspace_client()
        
        # List all connections and filter for SharePoint type
        all_connections = list(w.connections.list())
        
        sharepoint_connections = []
        for conn in all_connections:
            # Filter for SharePoint connections by name pattern (since SHAREPOINT_ONLINE type doesn't exist)
            # SharePoint connections typically have "sharepoint" in their name or use HTTP connection type
            conn_name_lower = conn.name.lower() if conn.name else ""
            is_sharepoint = "sharepoint" in conn_name_lower
            
            if is_sharepoint:
                connection_info = {
                    "id": conn.name,  # Connection name is the unique identifier
                    "name": conn.name,
                    "connection_name": conn.name,
                    "connection_type": conn.connection_type.value if conn.connection_type else "HTTP",
                    "comment": conn.comment or "",
                    "site_id": "",  # Extract from comment if stored there
                    "tenant_id": "",  # Not directly exposed in connection object
                    "created_by": conn.owner if hasattr(conn, 'owner') else "",
                }
                
                # Try to extract site_id from comment
                if conn.comment:
                    # Comment format might be "Site ID: <uuid>" or just the site ID
                    comment_lower = conn.comment.lower()
                    if "site" in comment_lower or "id" in comment_lower:
                        # Try to extract UUID pattern
                        import re
                        uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
                        match = re.search(uuid_pattern, conn.comment, re.IGNORECASE)
                        if match:
                            connection_info["site_id"] = match.group(0)
                        else:
                            connection_info["site_id"] = conn.comment
                
                sharepoint_connections.append(connection_info)
        
        return sharepoint_connections
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list SharePoint connections: {str(e)}"
        )


@router.post("/connections")
async def create_sharepoint_connection(connection: SharePointConnectionCreate) -> Dict[str, Any]:
    """
    Create a new SharePoint connection in Unity Catalog.
    
    Creates a Unity Catalog connection with OAuth User-to-Machine (U2M) credentials.
    """
    try:
        w = _get_workspace_client()
        
        # Build connection options for SharePoint OAuth U2M
        options = {
            "client_id": connection.client_id,
            "client_secret": connection.client_secret,
            "tenant_id": connection.tenant_id,
            "refresh_token": connection.refresh_token,
        }
        
        # Store site_id in comment for reference
        comment = f"Site ID: {connection.site_id}" if connection.site_id else "SharePoint connection created via API"
        
        # Create the connection
        # Note: SharePoint uses HTTP connection type (SHAREPOINT_ONLINE doesn't exist in SDK)
        created_connection = w.connections.create(
            name=connection.connection_name,
            connection_type=ConnectionType.HTTP,
            options=options,
            comment=comment
        )
        
        return {
            "message": "SharePoint connection created successfully",
            "connection_id": created_connection.name,
            "connection_name": created_connection.name,
            "connection_type": created_connection.connection_type.value if created_connection.connection_type else "SHAREPOINT_ONLINE"
        }
        
    except Exception as e:
        # Check for common errors
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            raise HTTPException(
                status_code=400,
                detail=f"Connection '{connection.connection_name}' already exists"
            )
        elif "invalid" in error_msg.lower() or "credential" in error_msg.lower():
            raise HTTPException(
                status_code=400,
                detail=f"Invalid credentials: {error_msg}"
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create SharePoint connection: {error_msg}"
            )


@router.delete("/connections/{connection_id}")
async def delete_sharepoint_connection(connection_id: str) -> Dict[str, Any]:
    """
    Delete a SharePoint connection from Unity Catalog.
    
    Note: This will fail if the connection is in use by any tables or pipelines.
    """
    try:
        w = _get_workspace_client()
        
        # Delete the connection
        w.connections.delete(name=connection_id)
        
        return {
            "message": "SharePoint connection deleted successfully",
            "connection_id": connection_id
        }
        
    except Exception as e:
        error_msg = str(e)
        if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
            raise HTTPException(
                status_code=404,
                detail=f"Connection '{connection_id}' not found"
            )
        elif "in use" in error_msg.lower() or "referenced" in error_msg.lower():
            raise HTTPException(
                status_code=400,
                detail=f"Connection '{connection_id}' is in use and cannot be deleted"
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete SharePoint connection: {error_msg}"
            )


@router.post("/connections/{connection_id}/test")
async def test_sharepoint_connection(connection_id: str) -> Dict[str, Any]:
    """
    Test a SharePoint connection by attempting to list sites.
    
    Note: This is a placeholder - actual testing would require making a SharePoint API call.
    """
    try:
        w = _get_workspace_client()
        
        # Get the connection to verify it exists
        connection = w.connections.get(name=connection_id)
        
        # Check if it's a SharePoint connection by name pattern
        if "sharepoint" not in connection_id.lower():
            raise HTTPException(
                status_code=400,
                detail=f"Connection '{connection_id}' is not a SharePoint connection"
            )
        
        # For now, just return success if connection exists
        # A real test would execute a query against SharePoint
        return {
            "success": True,
            "message": f"Connection '{connection_id}' exists and is configured",
            "connection_type": connection.connection_type.value
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        if "not found" in error_msg.lower():
            raise HTTPException(
                status_code=404,
                detail=f"Connection '{connection_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to test connection: {error_msg}"
            )
