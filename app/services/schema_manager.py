# app/services/schema_manager.py
"""
SchemaManager Service - Infrastructure as Code for database schema initialization.
Uses Unity Catalog SDK and Databricks Tools Core to ensure tables exist on startup.
Replaces scattered CREATE TABLE IF NOT EXISTS SQL strings with centralized management.
"""
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo, TableType, DataSourceFormat, ColumnTypeName
from typing import List, Dict
import asyncio


class _SchemaManagerService:
    """Singleton service for managing database schema initialization."""

    _instance = None
    _workspace_client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_SchemaManagerService, cls).__new__(cls)
        return cls._instance

    def _get_workspace_client(self) -> WorkspaceClient:
        """Get or create Databricks Workspace Client."""
        if self._workspace_client is None:
            self._workspace_client = WorkspaceClient()
        return self._workspace_client

    async def _ensure_catalog_and_schema_exist(
        self, catalog: str, schema: str
    ) -> None:
        """
        Ensure catalog and schema exist, create if they don't.
        
        Args:
            catalog: Catalog name
            schema: Schema name
        """
        w = self._get_workspace_client()

        # Check and create catalog
        def check_catalog():
            try:
                w.catalogs.get(name=catalog)
                return True
            except Exception:
                return False

        def create_catalog():
            try:
                w.catalogs.create(name=catalog, comment=f"Auto-created by SchemaManager")
                return True
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise
                return True

        catalog_exists = await asyncio.to_thread(check_catalog)
        if not catalog_exists:
            await asyncio.to_thread(create_catalog)

        # Check and create schema
        def check_schema():
            try:
                w.schemas.get(full_name=f"{catalog}.{schema}")
                return True
            except Exception:
                return False

        def create_schema():
            try:
                w.schemas.create(
                    name=schema, catalog_name=catalog, comment=f"Auto-created by SchemaManager"
                )
                return True
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise
                return True

        schema_exists = await asyncio.to_thread(check_schema)
        if not schema_exists:
            await asyncio.to_thread(create_schema)

    async def _ensure_table_exists(
        self, catalog: str, schema: str, table_name: str, columns: List[ColumnInfo], comment: str = ""
    ) -> None:
        """
        Ensure a table exists with the specified schema.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table_name: Table name
            columns: List of ColumnInfo objects defining the table schema
            comment: Optional table comment
        """
        w = self._get_workspace_client()
        full_name = f"{catalog}.{schema}.{table_name}"

        def check_and_create_table():
            # Check if table exists
            try:
                exists = w.tables.exists(full_name=full_name)
                if exists and exists.table_exists:
                    return True
            except Exception:
                pass

            # Create table if it doesn't exist
            try:
                table_comment = comment or f"Auto-created by SchemaManager"
                # #region agent log
                import json;open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log','a').write(json.dumps({"location":"schema_manager.py:114","message":"Before table create","data":{"catalog":catalog,"schema":schema,"table_name":table_name,"table_type":str(TableType.MANAGED),"data_source_format":str(DataSourceFormat.DELTA),"has_columns":len(columns)>0},"timestamp":__import__('time').time()*1000,"sessionId":"debug-session","hypothesisId":"H1-H3"})+'\n')
                # #endregion
                w.tables.create(
                    catalog_name=catalog,
                    schema_name=schema,
                    name=table_name,
                    columns=columns,
                    table_type=TableType.MANAGED,
                    data_source_format=DataSourceFormat.DELTA,
                    storage_location="",  # Empty for MANAGED tables
                    properties={"comment": table_comment},
                )
                # #region agent log
                open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log','a').write(json.dumps({"location":"schema_manager.py:127","message":"Table created successfully","data":{"full_name":full_name},"timestamp":__import__('time').time()*1000,"sessionId":"debug-session","hypothesisId":"SUCCESS"})+'\n')
                # #endregion
                return True
            except Exception as e:
                # #region agent log
                import traceback;open('/Users/dastan.aitzhanov/projects/fe-vibe-app/.cursor/debug.log','a').write(json.dumps({"location":"schema_manager.py:133","message":"Table create failed","data":{"full_name":full_name,"error":str(e),"error_type":type(e).__name__,"traceback":traceback.format_exc()},"timestamp":__import__('time').time()*1000,"sessionId":"debug-session","hypothesisId":"H1-H5"})+'\n')
                # #endregion
                if "already exists" not in str(e).lower():
                    raise Exception(f"Failed to create table {full_name}: {str(e)}")
                return True

        await asyncio.to_thread(check_and_create_table)

    async def initialize_sharepoint_tables(self) -> Dict[str, bool]:
        """
        Initialize SharePoint-related tables in Unity Catalog.
        
        Returns:
            Dict with table names and creation status
        """
        catalog = os.getenv("UC_CATALOG", "main")
        schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")

        # Ensure catalog and schema exist
        await self._ensure_catalog_and_schema_exist(catalog, schema)

        # Define sharepoint_connections table schema
        connections_columns = [
            ColumnInfo(name="id", type_name=ColumnTypeName.STRING, comment="Connection ID"),
            ColumnInfo(name="name", type_name=ColumnTypeName.STRING, comment="Connection display name"),
            ColumnInfo(name="client_id", type_name=ColumnTypeName.STRING, comment="OAuth client ID"),
            ColumnInfo(name="client_secret", type_name=ColumnTypeName.STRING, comment="OAuth client secret"),
            ColumnInfo(name="tenant_id", type_name=ColumnTypeName.STRING, comment="Azure tenant ID"),
            ColumnInfo(name="refresh_token", type_name=ColumnTypeName.STRING, comment="OAuth refresh token"),
            ColumnInfo(name="site_id", type_name=ColumnTypeName.STRING, comment="SharePoint site ID"),
            ColumnInfo(name="connection_name", type_name=ColumnTypeName.STRING, comment="Databricks connection name"),
        ]

        # Define sharepoint_pipelines table schema
        pipelines_columns = [
            ColumnInfo(name="id", type_name=ColumnTypeName.STRING, comment="Pipeline ID"),
            ColumnInfo(name="name", type_name=ColumnTypeName.STRING, comment="Pipeline display name"),
            ColumnInfo(name="connection_id", type_name=ColumnTypeName.STRING, comment="Reference to connection ID"),
            ColumnInfo(name="ingestion_type", type_name=ColumnTypeName.STRING, comment="Type of ingestion"),
            ColumnInfo(name="drive_names", type_name=ColumnTypeName.STRING, comment="JSON array of drive names"),
            ColumnInfo(name="delta_table", type_name=ColumnTypeName.STRING, comment="Destination Delta table"),
            ColumnInfo(name="file_pattern", type_name=ColumnTypeName.STRING, comment="File pattern to match"),
            ColumnInfo(name="pipeline_id", type_name=ColumnTypeName.STRING, comment="Databricks pipeline ID"),
        ]

        # Create tables
        await self._ensure_table_exists(
            catalog, schema, "sharepoint_connections", connections_columns, "SharePoint connection configurations"
        )
        await self._ensure_table_exists(
            catalog, schema, "sharepoint_pipelines", pipelines_columns, "SharePoint pipeline configurations"
        )

        return {
            "sharepoint_connections": True,
            "sharepoint_pipelines": True,
        }

    async def initialize_lakebase_tables(self) -> Dict[str, bool]:
        """
        Initialize Lakebase-related tables (Excel streaming configs).
        Note: These are in Lakebase PostgreSQL, not Unity Catalog.
        
        For now, we'll keep the CREATE TABLE IF NOT EXISTS approach for Lakebase
        since it's a different system. This method is a placeholder for future migration.
        
        Returns:
            Dict with table names and status
        """
        # Lakebase uses PostgreSQL, not Unity Catalog
        # The CREATE TABLE IF NOT EXISTS approach remains valid for this use case
        # This is documented here for completeness
        return {
            "excel_stream_configs": True,  # Created via Lakebase.query() in routes
        }


# Create singleton instance
SchemaManager = _SchemaManagerService()
