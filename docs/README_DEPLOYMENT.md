# Databricks Asset Bundle Deployment

## Prerequisites

1. Databricks CLI installed: `pip install databricks-cli`
2. Databricks workspace with Unity Catalog enabled
3. Personal Access Token (PAT)
4. SQL Warehouse configured

## Development Model

This bundle uses a **hybrid deployment approach**:

- **Deployed to Databricks**: Notebooks and orchestration jobs (automated data processing)
- **Run Locally**: FastAPI web UI for interactive development and testing

### Local Development

1. Start the FastAPI app locally:
   ```bash
   uvicorn app.main:app --reload
   ```

2. Access the UI at: `http://localhost:8000`

3. The local app will connect to your Databricks workspace using credentials from `.env`

### Databricks Deployment

The bundle deploys only backend automation (notebooks + jobs) to Databricks:
- Notebooks in `/Workspace/Users/your-email/.bundle/sharepoint-lakeflow-bundle/dev/`
- Scheduled jobs for orchestration

### Future: Deploy UI to Databricks

When ready to deploy the UI as a Databricks App, uncomment the `apps:` section in `databricks.yml`.

## Setup

### 1. Configure Databricks CLI

```bash
databricks configure --token
```

Enter your workspace URL and PAT when prompted.

### 2. Copy and Configure Environment

```bash
cp .env.template .env
# Edit .env with your values
```

Update the following in `.env`:
- `DATABRICKS_HOST`: Your workspace URL
- `DATABRICKS_TOKEN`: Your Personal Access Token
- `DATABRICKS_WAREHOUSE_ID`: Your SQL Warehouse ID
- `MY_EMAIL`: Your email for notifications

### 3. Validate Bundle

```bash
databricks bundle validate
```

This checks your bundle configuration for errors.

## Deployment

### Deploy to Development

```bash
databricks bundle deploy -t dev
```

This will:
- Deploy notebooks to `/Workspace/Users/your-email/sharepoint-lakeflow-bundle/`
- Create the Lakeflow Orchestration job
- Deploy the Databricks App

### Deploy to Production

```bash
databricks bundle deploy -t prod
```

Production deployment includes additional permissions and configurations.

## Access the App

After deployment, access the Databricks App at:

```
https://your-workspace.cloud.databricks.com/apps/sharepoint-lakeflow
```

## Managing Resources

### List Deployed Resources

```bash
databricks bundle resources list
```

Shows all resources managed by the bundle (jobs, apps, notebooks).

### Run a Job Manually

```bash
databricks jobs run-now --job-id <job-id>
```

Get the job ID from the `databricks bundle resources list` output.

### View Job Runs

```bash
databricks jobs list-runs --job-id <job-id>
```

### Update Deployment

After making changes to code:

```bash
databricks bundle deploy -t dev
```

The bundle will update only changed resources.

### Destroy All Resources

```bash
databricks bundle destroy -t dev
```

**Warning**: This removes all deployed resources (jobs, apps, notebooks).

## Bundle Structure

```
fe-vibe-app/
├── databricks.yml              # Bundle configuration
├── src/
│   ├── notebooks/              # Databricks notebooks
│   │   ├── connection_management.py
│   │   └── lakeflow_management.py
│   └── app/                    # Databricks App
│       ├── app.py              # Flask backend
│       ├── requirements.txt
│       └── templates/
│           └── index.html      # UI
├── .env.template               # Environment template
└── README_DEPLOYMENT.md        # This file
```

## Key Features

### Notebooks

- **connection_management.py**: Lists and manages SharePoint connections from Unity Catalog
- **lakeflow_management.py**: Creates Lakeflow pipelines and scheduled jobs

### Databricks App

- Web UI for managing SharePoint connections
- Create Lakeflow jobs with 15-minute schedules
- View and delete existing jobs
- Uses Flask backend with Databricks SDK

### Jobs

- **Lakeflow Orchestration**: Scheduled job that refreshes connections and manages pipelines
- Runs daily at midnight UTC
- Email notifications on failure

## Troubleshooting

### Bundle Validation Fails

Check:
- YAML syntax in `databricks.yml`
- File paths are correct
- All required variables are defined

### App Not Accessible

1. Check app deployment status:
   ```bash
   databricks apps list
   ```

2. View app logs:
   ```bash
   databricks apps logs sharepoint-lakeflow-ui
   ```

### Job Failures

1. Check job run details:
   ```bash
   databricks jobs get-run --run-id <run-id>
   ```

2. View notebook output in the Databricks UI

## Development Workflow

1. Make changes locally
2. Test locally if possible
3. Deploy to dev:
   ```bash
   databricks bundle deploy -t dev
   ```
4. Test in dev environment
5. Deploy to prod:
   ```bash
   databricks bundle deploy -t prod
   ```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Deploy to Databricks

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Databricks CLI
        run: |
          pip install databricks-cli
          
      - name: Deploy Bundle
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks bundle deploy -t prod
```

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Databricks Apps Documentation](https://docs.databricks.com/apps/)
- [Lakeflow Connect Documentation](https://docs.databricks.com/ingestion/lakeflow-connect/)
