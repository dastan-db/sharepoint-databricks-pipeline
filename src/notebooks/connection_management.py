# Databricks notebook source
# MAGIC %md
# MAGIC # SharePoint Connection Management
# MAGIC 
# MAGIC This notebook manages SharePoint connections in Unity Catalog

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.70.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Workspace Client

# COMMAND ----------

w = WorkspaceClient()
catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "main"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "vibe_coding"
warehouse_id = dbutils.widgets.get("warehouse_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List SharePoint Connections

# COMMAND ----------

def list_sharepoint_connections():
    """List all SharePoint connections from Unity Catalog"""
    sql = "SHOW CONNECTIONS"
    
    statement = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s"
    )
    
    if statement.status.state == StatementState.SUCCEEDED:
        connections = []
        if statement.result and statement.result.data_array:
            columns = [col.name for col in statement.manifest.schema.columns]
            for row in statement.result.data_array:
                conn_dict = dict(zip(columns, row))
                if conn_dict.get('type', '').upper() == 'SHAREPOINT':
                    connections.append(conn_dict)
        return connections
    return []

# COMMAND ----------

# Display connections
connections = list_sharepoint_connections()
display(connections)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Connection Metadata
# MAGIC Store connection info in a Delta table for app use

# COMMAND ----------

from pyspark.sql import Row

if connections:
    connection_rows = [
        Row(
            name=c.get('name'),
            type=c.get('type'),
            comment=c.get('comment', ''),
            created_at=c.get('created_at')
        ) for c in connections
    ]
    
    df = spark.createDataFrame(connection_rows)
    
    # Write to Delta table
    table_name = f"{catalog}.{schema}.sharepoint_connections_metadata"
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    
    print(f"Stored {len(connections)} connections to {table_name}")
