# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Job Management
# MAGIC 
# MAGIC This notebook manages Lakeflow ingestion jobs

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.70.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings as Job
from datetime import datetime
import json

# COMMAND ----------

w = WorkspaceClient()
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Lakeflow Job

# COMMAND ----------

def create_lakeflow_job(config):
    """
    Create a Lakeflow pipeline and scheduled job
    
    Args:
        config: dict with keys:
            - name: Job name
            - connection_name: SharePoint connection name
            - source_schema: Site ID
            - destination_catalog: Target catalog
            - destination_schema: Target schema
    """
    
    # Create pipeline
    pipeline_spec = {
        "name": config["name"],
        "ingestion_definition": {
            "connection_name": config["connection_name"],
            "objects": [{
                "schema": {
                    "source_schema": config["source_schema"],
                    "destination_catalog": config["destination_catalog"],
                    "destination_schema": config["destination_schema"]
                }
            }],
            "source_type": "SHAREPOINT"
        },
        "target": config["destination_schema"],
        "channel": "PREVIEW",
        "catalog": config["destination_catalog"],
    }
    
    pipeline = w.pipelines.create(**pipeline_spec)
    
    # Create scheduled job
    job_settings = Job.from_dict({
        "name": config["name"],
        "email_notifications": {
            "on_failure": [config.get("notification_email", "")],
            "no_alert_for_skipped_runs": True,
        },
        "trigger": {
            "pause_status": "UNPAUSED",
            "periodic": {
                "interval": 15,
                "unit": "MINUTES",
            },
        },
        "tasks": [{
            "task_key": f"pipeline-task-{pipeline.pipeline_id}",
            "pipeline_task": {
                "pipeline_id": pipeline.pipeline_id,
            },
        }],
        "performance_target": "PERFORMANCE_OPTIMIZED",
    })
    
    job = w.jobs.create(**job_settings.as_shallow_dict())
    
    return {
        "pipeline_id": pipeline.pipeline_id,
        "job_id": job.job_id,
        "created_at": datetime.utcnow().isoformat()
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: Create a Job

# COMMAND ----------

# Uncomment to test
# config = {
#     "name": "Test Lakeflow Job",
#     "connection_name": "sharepoint-fe",
#     "source_schema": "site-id-here",
#     "destination_catalog": "main",
#     "destination_schema": "test_schema",
#     "notification_email": "your.email@company.com"
# }
# result = create_lakeflow_job(config)
# print(result)
