from fastapi import APIRouter, HTTPException
from app.core.models import LakeflowJobConfig
from app.services.lakebase import Lakebase
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings as Job
import os
from datetime import datetime

router = APIRouter()


def _get_lakeflow_jobs_table():
    """Get fully qualified table name for lakeflow jobs"""
    catalog = os.getenv("LAKEBASE_CATALOG", "main")
    schema = os.getenv("LAKEBASE_SCHEMA", "vibe_coding")
    return f"{schema}.lakeflow_jobs"


@router.get("/jobs")
async def list_lakeflow_jobs():
    """List all Lakeflow jobs"""
    try:
        jobs_table = _get_lakeflow_jobs_table()
        query = f"""
            SELECT id, name, connection_id, connection_name, source_schema,
                   destination_catalog, destination_schema, pipeline_id, job_id, created_at
            FROM {jobs_table}
            ORDER BY created_at DESC
        """
        rows = Lakebase.query(query)
        
        jobs = []
        for row in rows:
            jobs.append(LakeflowJobConfig(
                id=row[0],
                name=row[1],
                connection_id=row[2],
                connection_name=row[3],
                source_schema=row[4],
                destination_catalog=row[5],
                destination_schema=row[6],
                pipeline_id=row[7],
                job_id=row[8],
                created_at=row[9],
            ))
        return jobs
    except Exception as e:
        return []


@router.post("/jobs")
async def create_lakeflow_job(config: LakeflowJobConfig):
    """Create a new Lakeflow ingestion job"""
    try:
        jobs_table = _get_lakeflow_jobs_table()
        
        # Create table if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {jobs_table} (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                connection_id VARCHAR NOT NULL,
                connection_name VARCHAR NOT NULL,
                source_schema VARCHAR NOT NULL,
                destination_catalog VARCHAR NOT NULL,
                destination_schema VARCHAR NOT NULL,
                pipeline_id VARCHAR,
                job_id VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        Lakebase.query(create_table_query)
        
        # Create the Databricks pipeline
        w = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        )
        
        # Build pipeline spec based on YAML structure
        pipeline_spec = {
            "name": config.name,
            "ingestion_definition": {
                "connection_name": config.connection_name,
                "objects": [{
                    "schema": {
                        "source_schema": config.source_schema,
                        "destination_catalog": config.destination_catalog,
                        "destination_schema": config.destination_schema
                    }
                }],
                "source_type": "SHAREPOINT"
            },
            "target": config.destination_schema,
            "channel": "PREVIEW",
            "catalog": config.destination_catalog,
        }
        
        # Create pipeline via Databricks API
        pipeline = w.pipelines.create(**pipeline_spec)
        config.pipeline_id = pipeline.pipeline_id
        
        # Create a scheduled job to run the pipeline
        user_email = os.getenv("MY_EMAIL", "")
        job_settings = Job.from_dict({
            "name": config.name,
            "email_notifications": {
                "on_failure": [user_email] if user_email else [],
                "no_alert_for_skipped_runs": True,
            },
            "notification_settings": {
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
                "task_key": f"pipeline-task-{config.pipeline_id}",
                "pipeline_task": {
                    "pipeline_id": config.pipeline_id,
                },
            }],
            "performance_target": "PERFORMANCE_OPTIMIZED",
        })
        
        # Create the job
        job = w.jobs.create(**job_settings.as_shallow_dict())
        config.job_id = str(job.job_id)
        config.created_at = datetime.utcnow().isoformat()
        
        # Store job config
        insert_query = f"""
            INSERT INTO {jobs_table}
            (id, name, connection_id, connection_name, source_schema,
             destination_catalog, destination_schema, pipeline_id, job_id, created_at)
            VALUES ('{config.id}', '{config.name}', '{config.connection_id}',
                    '{config.connection_name}', '{config.source_schema}',
                    '{config.destination_catalog}', '{config.destination_schema}',
                    '{config.pipeline_id}', '{config.job_id}', '{config.created_at}')
            RETURNING *
        """
        Lakebase.query(insert_query)
        
        return {
            "message": "Lakeflow job created successfully",
            "id": config.id,
            "pipeline_id": config.pipeline_id,
            "job_id": config.job_id
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create Lakeflow job: {str(e)}")


@router.delete("/jobs/{job_id}")
async def delete_lakeflow_job(job_id: str):
    """Delete a Lakeflow job"""
    try:
        jobs_table = _get_lakeflow_jobs_table()
        query = f"DELETE FROM {jobs_table} WHERE id = '{job_id}' RETURNING *"
        Lakebase.query(query)
        return {"message": "Lakeflow job deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")
