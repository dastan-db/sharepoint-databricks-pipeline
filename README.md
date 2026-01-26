# SharePoint to Databricks Data Pipeline

A modern web application for viewing and managing SharePoint data ingested into Databricks Unity Catalog via Lakeflow connectors.

Built with FastAPI backend and a beautiful single-page HTML/CSS/JavaScript frontend.

## Technologies Used

-   **FastAPI**: High-performance web framework for building APIs
-   **Uvicorn**: ASGI server for running the FastAPI application
-   **Python-dotenv**: Environment variable management
-   **Databricks SDK**: Integration with Databricks Unity Catalog and SQL Warehouses
-   **Unity Catalog**: Access SharePoint data ingested by Lakeflow connectors
-   **Pandas**: Excel file parsing and data manipulation
-   **HTML5/CSS3/JavaScript**: Modern, responsive single-page application

## Architecture Overview

```
┌──────────────┐     ┌─────────────────┐     ┌────────────────────┐
│  SharePoint  │────>│ Lakeflow        │────>│  Unity Catalog     │
│  Data Sources│     │ Connectors      │     │  Tables            │
└──────────────┘     └─────────────────┘     └────────────────────┘
                                                       │
                                                       ↓
                              ┌────────────────────────────────────────┐
                              │     Web Application (FastAPI)          │
                              │  ┌──────────────────────────────────┐  │
                              │  │  Unity Catalog Service           │  │
                              │  │  (SQL Warehouse Queries)         │  │
                              │  └──────────────────────────────────┘  │
                              └────────────────────────────────────────┘
                                               ↓
                              ┌────────────────────────────────────────┐
                              │   Web UI (HTML/CSS/JavaScript)         │
                              │   - View SharePoint connections         │
                              │   - Browse ingested data               │
                              └────────────────────────────────────────┘
```

### Data Flow

1. **Lakeflow Connectors**: Ingest SharePoint data into Unity Catalog tables (managed by Databricks)
2. **Unity Catalog**: Stores SharePoint data in managed/streaming tables across various catalogs and schemas
3. **Web Application**: Queries Unity Catalog via SQL Warehouse to display SharePoint connections and data
4. **Web UI**: Interactive interface to view and manage SharePoint data sources

## Features

### Section 1: SharePoint Connections
-   **Connection Discovery**: Automatically discover all SharePoint tables in Unity Catalog (135 found)
-   **Connection Viewing**: Browse SharePoint data sources organized by catalog and schema
-   **Table Metadata**: View table names, types (MANAGED/STREAMING_TABLE), and full Unity Catalog paths
-   **User-friendly Display**: Clean interface showing connection details and available actions

### Section 2: Lakeflow Pipelines (Excel Ingestion)
-   **Managed Ingestion**: Databricks Lakeflow Connect pipelines for automated Excel ingestion
-   **Flexible Targeting**: Ingest from all drives or specific document libraries
-   **File Filtering**: Pattern-based filtering (e.g., *.xlsx) to ingest only Excel files
-   **Lakebase Destination**: All pipelines write to Lakebase documents table for staging
-   **Pipeline Monitoring**: View pipeline status and trigger manual runs

### Section 3: Excel Streaming Configurations
-   **Continuous Streaming**: Real-time data processing from Lakebase to Delta tables
-   **Pattern Matching**: Filter files by name pattern (e.g., supplier_*.xlsx)
-   **Configurable Triggers**: Set streaming frequency (10s, 30s, 1min, 5min)
-   **Checkpoint Management**: Fault-tolerant streaming with automatic checkpointing
-   **Start/Stop Control**: Enable or disable streams on demand
-   **Status Monitoring**: View active streams and their current state

### 🎨 User Experience

-   **Three Clear Sections**: Organized workflow from connections → ingestion → streaming
-   **Beautiful Design**: Modern gradient background with card-based layout
-   **Responsive**: Works perfectly on desktop, tablet, and mobile
-   **Real-time Feedback**: Instant status updates with visual indicators (🟢 Active / 🔴 Stopped)
-   **Loading States**: Visual feedback during operations

## Getting Started

1. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

2. Set up environment variables:

    - Copy `example.env` to `.env`
    - Fill in your actual Databricks credentials:
        - `DATABRICKS_HOST`
        - `DATABRICKS_CLIENT_ID`
        - `DATABRICKS_CLIENT_SECRET`
        - `LAKEBASE_INSTANCE_NAME`
        - `LAKEBASE_DB_NAME`
        - `LAKEBASE_CATALOG` (default: main)
        - `LAKEBASE_SCHEMA` (default: vibe_coding)

3. Run the application:

    ```bash
    uvicorn app.main:app --reload
    ```

4. Open your browser to `http://localhost:8000`

## API Endpoints

### SharePoint Connections

-   `GET /sharepoint/connections` - List all SharePoint connections
-   `POST /sharepoint/connections` - Create a new SharePoint connection
-   `GET /sharepoint/connections/{id}` - Get a specific connection
-   `DELETE /sharepoint/connections/{id}` - Delete a connection
-   `POST /sharepoint/connections/{id}/test` - Test connection credentials

### Lakeflow Pipelines (Excel Ingestion)

-   `GET /sharepoint/pipelines` - List all Lakeflow pipeline configurations
-   `POST /sharepoint/pipelines` - Create a new ingestion pipeline (targets Lakebase)
-   `GET /sharepoint/pipelines/{id}` - Get a specific pipeline
-   `DELETE /sharepoint/pipelines/{id}` - Delete a pipeline
-   `POST /sharepoint/pipelines/{id}/run` - Trigger a pipeline run
-   `GET /sharepoint/pipelines/{id}/status` - Get pipeline status and run details

### Excel Streaming Configurations

-   `GET /excel-streaming/configs` - List all streaming configurations
-   `POST /excel-streaming/configs` - Create a new streaming configuration
-   `GET /excel-streaming/configs/{id}` - Get a specific configuration
-   `PUT /excel-streaming/configs/{id}` - Update a configuration
-   `DELETE /excel-streaming/configs/{id}` - Delete a configuration
-   `POST /excel-streaming/configs/{id}/start` - Start continuous streaming
-   `POST /excel-streaming/configs/{id}/stop` - Stop streaming
-   `GET /excel-streaming/configs/{id}/status` - Get streaming status and metrics

### System

-   `GET /health` - Health check endpoint
-   `GET /docs` - Interactive API documentation

## Database Architecture

### Lakebase Connection

-   **Singleton Pattern**: Single connection instance with automatic token refresh
-   **OAuth Integration**: Databricks SDK authentication
-   **Token Refresh**: Automatic renewal every 59 minutes
-   **Postgres Protocol**: Uses psycopg2 for Lakebase connectivity

### Configuration Storage

All configurations are stored in Lakebase:

**SharePoint Connections:**
```sql
CREATE TABLE IF NOT EXISTS sharepoint_connections (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    client_id VARCHAR NOT NULL,
    client_secret VARCHAR NOT NULL,
    tenant_id VARCHAR NOT NULL,
    refresh_token VARCHAR NOT NULL,
    site_id VARCHAR NOT NULL,
    connection_name VARCHAR NOT NULL
);
```

**Lakeflow Pipelines:**
```sql
CREATE TABLE IF NOT EXISTS sharepoint_pipelines (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    connection_id VARCHAR NOT NULL,
    ingestion_type VARCHAR NOT NULL,
    drive_names VARCHAR,
    lakebase_table VARCHAR NOT NULL,
    file_pattern VARCHAR DEFAULT '*.xlsx',
    pipeline_id VARCHAR
);
```

**Excel Streaming Configurations:**
```sql
CREATE TABLE IF NOT EXISTS excel_stream_configs (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    lakebase_table VARCHAR NOT NULL,
    file_name_pattern VARCHAR NOT NULL,
    destination_catalog VARCHAR NOT NULL,
    destination_schema VARCHAR NOT NULL,
    destination_table VARCHAR NOT NULL,
    checkpoint_location VARCHAR NOT NULL,
    trigger_interval VARCHAR DEFAULT '10 seconds',
    is_active BOOLEAN DEFAULT false
);
```

## Complete Workflow

### Step 1: Create SharePoint Connection

1. Navigate to the **SharePoint Connections** section
2. Fill in OAuth U2M credentials:
   - Connection ID and Name
   - Azure Entra ID Client ID, Secret, and Tenant ID
   - OAuth Refresh Token
   - SharePoint Site ID
3. Click **Create Connection**
4. Test the connection to verify credentials

### Step 2: Create Lakeflow Pipeline

1. Navigate to the **Lakeflow Pipelines** section
2. Select your SharePoint connection from the dropdown
3. Choose ingestion type:
   - **All Drives**: Ingest all document libraries from the site
   - **Specific Drives**: Enter comma-separated drive names
4. Configure destination:
   - Lakebase Table (default: `documents`)
   - File Pattern (default: `*.xlsx`)
5. Click **Create Pipeline**
6. Trigger a manual run to start ingestion

The pipeline will ingest Excel files from SharePoint into your Lakebase documents table.

### Step 3: Create Excel Streaming Configuration

1. Navigate to the **Excel Streaming Configurations** section
2. Configure the stream:
   - Lakebase Table (source, matches pipeline destination)
   - File Name Pattern (e.g., `supplier_*.xlsx`)
   - Destination: catalog, schema, table for Delta
   - Checkpoint Location for streaming state
   - Trigger Interval (how often to check for new data)
3. Click **Create Stream Config**
4. Click **Start** to begin continuous streaming

The stream will continuously monitor Lakebase and process new Excel files into Delta tables.

### Monitoring

- **Pipeline Status**: Check Lakeflow pipeline runs and ingestion status
- **Stream Status**: View active streams, check if streaming is running
- **Data Validation**: Verify data is flowing through all three stages

## SharePoint OAuth U2M Setup

To use the SharePoint connector, you need to set up OAuth User-to-Machine (U2M) authentication with Microsoft Azure:

### Prerequisites
-   Unity Catalog enabled in your Databricks workspace
-   Serverless compute enabled
-   `CREATE CONNECTION` privileges on the metastore

### Steps to Configure OAuth U2M

1. **Create an Azure Entra ID App Registration**
   - Go to Azure Portal → Azure Active Directory → App registrations
   - Create a new registration
   - Note the **Application (client) ID** and **Directory (tenant) ID**

2. **Configure API Permissions**
   - Add Microsoft Graph API permissions:
     - `Sites.Read.All` (Delegated)
     - `Files.Read.All` (Delegated)
   - Grant admin consent for your organization

3. **Create Client Secret**
   - In your app registration, go to Certificates & secrets
   - Create a new client secret
   - Copy the secret value (you won't be able to see it again)

4. **Get SharePoint Site ID**
   - Navigate to your SharePoint site
   - Use the Microsoft Graph API or SharePoint API to retrieve your site ID
   - Format: `{hostname},{site-id},{web-id}`

5. **Generate Refresh Token**
   - Use OAuth 2.0 authorization code flow to obtain a refresh token
   - You'll need to authenticate as a user with access to the SharePoint site
   - The refresh token expires after 90 days by default

6. **Create Connection in App**
   - Use the web UI to create a SharePoint connection
   - Enter all the credentials obtained above
   - Test the connection to validate

### Lakeflow Pipeline Details

Lakeflow pipelines ingest Excel files from SharePoint to Lakebase:

- **Managed by Databricks**: Uses Lakeflow Connect for reliable ingestion
- **Automatic Schema Management**: Lakebase handles table creation
- **File Filtering**: Only ingests files matching the pattern (e.g., `*.xlsx`)
- **Incremental Updates**: Re-ingests only changed files on subsequent runs
- **Binary Storage**: Files stored in Lakebase documents table for streaming

### Excel Streaming Details

Excel streaming processes data from Lakebase to Delta tables:

- **Continuous Processing**: Monitors Lakebase for new/updated files
- **Checkpointing**: Fault-tolerant with automatic state management
- **Pattern Matching**: Filters files by name pattern within Lakebase
- **Configurable Frequency**: Control how often streaming checks for updates
- **Scalable**: Handles high-volume data ingestion

### Data Pipeline Benefits

- **Staged Architecture**: Clear separation between ingestion and processing
- **Reusable Connections**: One OAuth setup for multiple pipelines
- **Flexible Filtering**: Control what gets ingested and what gets streamed
- **Monitoring**: Track pipeline runs and stream status independently
- **Fault Tolerance**: Checkpointing ensures no data loss

### Security Considerations

⚠️ **SECURITY WARNING**: This demo uses f-string SQL queries for simplicity. In production, always use parameterized queries to prevent SQL injection attacks.

⚠️ **CREDENTIAL STORAGE**: Store sensitive credentials (client secrets, refresh tokens) encrypted in production. Consider using Databricks Secrets for enhanced security.

⚠️ **TOKEN EXPIRATION**: OAuth refresh tokens expire after 90 days by default. You'll need to refresh connections periodically.
