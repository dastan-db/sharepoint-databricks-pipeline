# app/services/excel_sync_notebook.py
"""
Excel Sync Notebook Generator - Creates notebook code for CDC-style Excel sync.
The generated notebook runs as a downstream task after SharePoint ingestion,
checking if the tracked Excel file was modified and syncing to Delta table.
"""
from typing import List, Optional
import os


class _ExcelSyncNotebookService:
    """Service for generating Excel sync notebook code."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_ExcelSyncNotebookService, cls).__new__(cls)
        return cls._instance

    def generate_sync_notebook(
        self,
        document_table: str,
        tracked_file_path: str,
        target_table: str,
        header_row: int = 0,
        selected_columns: Optional[List[str]] = None,
    ) -> str:
        """
        Generate Python notebook code for Excel sync task.
        
        Args:
            document_table: Full path to documents table (catalog.schema.documents)
            tracked_file_path: file_id of the Excel file to track
            target_table: Full path to target Delta table
            header_row: Which row contains headers (0-indexed)
            selected_columns: List of column names to include (None = all)
            
        Returns:
            Python notebook code as string
        """
        # Build column filter logic
        if selected_columns:
            columns_filter = f"selected_columns = {selected_columns}"
            columns_logic = """# Filter to selected columns only
if selected_columns:
    available_cols = [col for col in selected_columns if col in df.columns]
    if available_cols:
        df = df[available_cols]"""
        else:
            columns_filter = "selected_columns = None  # Use all columns"
            columns_logic = "# Using all columns from Excel"

        notebook_code = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Excel Sync Notebook - Auto-generated
# MAGIC 
# MAGIC This notebook syncs an Excel file from SharePoint to a Delta table.
# MAGIC It runs as a downstream task after SharePoint document ingestion.
# MAGIC 
# MAGIC **CDC Logic**: Only syncs when the tracked file has been modified.

# COMMAND ----------

# Configuration
document_table = "{document_table}"
tracked_file_path = "{tracked_file_path}"
target_table = "{target_table}"
header_row = {header_row}
{columns_filter}

print(f"Document Table: {{document_table}}")
print(f"Tracked File: {{tracked_file_path}}")
print(f"Target Table: {{target_table}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Check if file was modified (CDC)

# COMMAND ----------

from pyspark.sql.functions import col, max as spark_max

# Get current file modification time from documents table
file_info_df = spark.sql(f"""
    SELECT 
        file_metadata.last_modified_timestamp as last_modified,
        file_metadata.name as file_name
    FROM {{document_table}}
    WHERE file_id = '{{tracked_file_path}}'
    AND is_deleted = false
""")

if file_info_df.count() == 0:
    print(f"WARNING: File not found in documents table: {{tracked_file_path}}")
    dbutils.notebook.exit("FILE_NOT_FOUND")

file_info = file_info_df.first()
current_mod_time = file_info['last_modified']
file_name = file_info['file_name']

print(f"File: {{file_name}}")
print(f"Last Modified: {{current_mod_time}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check watermark (last sync time)

# COMMAND ----------

# Try to get watermark from target table properties
try:
    # Check if target table exists and get its properties
    target_exists = spark.catalog.tableExists(target_table)
    
    if target_exists:
        # Get table properties for watermark
        props_df = spark.sql(f"DESCRIBE EXTENDED {{target_table}}")
        watermark_row = props_df.filter(col("col_name") == "excel_sync_watermark").first()
        
        if watermark_row:
            from datetime import datetime
            watermark = datetime.fromisoformat(watermark_row['data_type'])
            print(f"Last sync watermark: {{watermark}}")
            
            if current_mod_time <= watermark:
                print("File has not been modified since last sync. Skipping.")
                dbutils.notebook.exit("NO_CHANGES")
        else:
            print("No watermark found. Will perform full sync.")
            watermark = None
    else:
        print(f"Target table {{target_table}} does not exist. Will create it.")
        watermark = None
except Exception as e:
    print(f"Could not check watermark: {{e}}")
    watermark = None

print("Proceeding with sync...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Read Excel content from documents table

# COMMAND ----------

import pandas as pd
import io
import base64

# Read file content
content_df = spark.sql(f"""
    SELECT content
    FROM {{document_table}}
    WHERE file_id = '{{tracked_file_path}}'
    AND is_deleted = false
    LIMIT 1
""")

content_row = content_df.first()
if content_row is None:
    print("ERROR: Could not read file content")
    dbutils.notebook.exit("READ_ERROR")

# Extract content - handle Row objects (complex types)
file_content = content_row['content']

# If content is a Row (struct), extract the bytes field
if hasattr(file_content, 'asDict'):
    content_dict = file_content.asDict()
    # Try common field names for binary content (inline_content is from SharePoint Lakeflow)
    for field_name in ['inline_content', 'bytes', 'data', 'value', 'content', 'binary']:
        if field_name in content_dict:
            file_content = content_dict[field_name]
            print(f"Extracted binary content from '{{field_name}}' field")
            break

# Handle different content types
if isinstance(file_content, str):
    # Base64 encoded string
    file_content = base64.b64decode(file_content)
elif isinstance(file_content, bytes):
    # Already bytes, use as-is
    pass
elif isinstance(file_content, bytearray):
    # Convert bytearray to bytes
    file_content = bytes(file_content)
else:
    print(f"ERROR: Could not convert content to bytes. Type: {{type(file_content)}}")
    dbutils.notebook.exit("CONTENT_TYPE_ERROR")

print(f"Read {{len(file_content)}} bytes from documents table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Parse Excel with pandas

# COMMAND ----------

# Install openpyxl if not already installed
%pip install -q openpyxl

# COMMAND ----------

# Parse Excel
df = pd.read_excel(
    io.BytesIO(file_content),
    header=header_row,
    engine='openpyxl'
)

print(f"Parsed {{len(df)}} rows, {{len(df.columns)}} columns")
print(f"Columns: {{list(df.columns)}}")
{columns_logic}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Convert to Spark DataFrame and write to Delta

# COMMAND ----------

# Convert pandas to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Show schema
print("Schema:")
spark_df.printSchema()

# COMMAND ----------

# Write to Delta table (overwrite mode for simplicity)
# For more sophisticated CDC, you could use MERGE INTO
spark_df.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .saveAsTable(target_table)

print(f"Successfully wrote {{len(df)}} rows to {{target_table}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Update watermark

# COMMAND ----------

# Store watermark as table property
spark.sql(f"""
    ALTER TABLE {{target_table}}
    SET TBLPROPERTIES ('excel_sync_watermark' = '{{current_mod_time.isoformat()}}')
""")

print(f"Updated watermark to: {{current_mod_time}}")
print("Sync completed successfully!")

dbutils.notebook.exit("SUCCESS")
'''
        return notebook_code

    def get_notebook_path(self, connection_id: str) -> str:
        """Get the workspace path for a sync notebook."""
        return f"/Workspace/Shared/excel_sync/{connection_id}_sync"


# Create singleton instance
ExcelSyncNotebook = _ExcelSyncNotebookService()
