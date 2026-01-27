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
import json
import time


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
                w.tables.create(
                    catalog_name=catalog,
                    schema_name=schema,
                    name=table_name,
                    columns=columns,
                    table_type=TableType.MANAGED,
                    data_source_format=DataSourceFormat.DELTA,
                    properties={"comment": table_comment},
                    # Don't specify storage_location or comment at top level for MANAGED tables
                )
                return True
            except Exception as e:
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
        # Legacy tables removed - now using native Unity Catalog connections via Lakeflow
        return {}

    async def initialize_lakebase_tables(self) -> Dict[str, bool]:
        """
        Initialize Lakebase-related tables.
        
        Returns:
            Dict with table names and status
        """
        # Legacy Excel streaming tables removed
        return {}


# Create singleton instance
SchemaManager = _SchemaManagerService()
