# Databricks App backend using Flask
from flask import Flask, render_template, request, jsonify
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from databricks.sdk.service.jobs import JobSettings as Job
import os

app = Flask(__name__)
w = WorkspaceClient()

@app.route('/')
def index():
    """Serve the main UI"""
    return render_template('index.html')

@app.route('/api/lakeflow/jobs', methods=['GET'])
def get_lakeflow_jobs():
    """List all Lakeflow jobs"""
    # TODO: Query from Delta table or Jobs API
    return jsonify([])

@app.route('/api/lakeflow/jobs', methods=['POST'])
def create_lakeflow_job():
    """Create a new Lakeflow job"""
    config = request.json
    
    try:
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
        user_email = os.getenv("MY_EMAIL", "")
        job_settings = Job.from_dict({
            "name": config["name"],
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
                "task_key": f"pipeline-task-{pipeline.pipeline_id}",
                "pipeline_task": {
                    "pipeline_id": pipeline.pipeline_id,
                },
            }],
            "performance_target": "PERFORMANCE_OPTIMIZED",
        })
        
        job = w.jobs.create(**job_settings.as_shallow_dict())
        
        return jsonify({
            "message": "Lakeflow job created successfully",
            "id": config["id"],
            "pipeline_id": pipeline.pipeline_id,
            "job_id": str(job.job_id)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/lakeflow/jobs/<job_id>', methods=['DELETE'])
def delete_lakeflow_job(job_id):
    """Delete a Lakeflow job"""
    # TODO: Implement job deletion
    return jsonify({"message": "Job deleted successfully"})

@app.route('/sharepoint/connections', methods=['GET'])
def get_connections():
    """List SharePoint connections"""
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    sql = "SHOW CONNECTIONS"
    
    try:
        statement = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="50s"
        )
        
        connections = []
        if statement.status.state == StatementState.SUCCEEDED:
            if statement.result and statement.result.data_array:
                columns = [col.name for col in statement.manifest.schema.columns]
                for row in statement.result.data_array:
                    conn_dict = dict(zip(columns, row))
                    if conn_dict.get('type', '').upper() == 'SHAREPOINT':
                        # Map to expected format
                        connections.append({
                            "id": conn_dict.get('name'),
                            "name": conn_dict.get('name'),
                            "site_id": conn_dict.get('comment', ''),
                            "type": conn_dict.get('type'),
                            "client_id": "",
                            "client_secret": "",
                            "tenant_id": "",
                            "refresh_token": "",
                            "connection_name": conn_dict.get('name')
                        })
        
        return jsonify(connections)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
