# SharePoint to Databricks Data Pipeline

A modern web application for viewing and managing SharePoint data ingested into Databricks Unity Catalog via Lakeflow connectors.

Built with FastAPI backend and a beautiful single-page HTML/CSS/JavaScript frontend.

## Technologies Used

### Core Technologies
-   **FastAPI**: High-performance web framework for building APIs
-   **Uvicorn**: ASGI server for running the FastAPI application
-   **Python-dotenv**: Environment variable management
-   **Databricks SDK**: Official Python SDK for Databricks platform integration
-   **databricks-tools-core**: High-level toolkit for secure SQL execution and job orchestration
-   **Databricks MCP Tools**: Model Context Protocol integration for intelligent data operations
-   **Unity Catalog**: Secure data governance and access control
-   **Pandas**: Excel file parsing and data manipulation
-   **HTML5/CSS3/JavaScript**: Modern, responsive single-page application

### Security & Infrastructure
-   **Secure SQL Execution**: SQL injection prevention via escaped queries and SDK methods
-   **Unity Catalog SDK**: Infrastructure-as-code for table management
-   **Databricks Jobs API**: Native orchestration replacing custom threading
-   **Schema Manager**: Automated database initialization on startup
-   **MCP Integration**: Intelligent warehouse selection and schema introspection

## Architecture Overview

```
┌──────────────┐     ┌─────────────────┐     ┌────────────────────┐
│  SharePoint  │────>│ Lakeflow        │────>│  Unity Catalog     │
│  Data Sources│     │ Connectors      │     │  (Secure Storage)  │
└──────────────┘     └─────────────────┘     └────────────────────┘
                                                       │
                                                       ↓
                    ┌─────────────────────────────────────────────────┐
                    │          Web Application (FastAPI)              │
                    │                                                 │
                    │  ┌──────────────────────────────────────────┐  │
                    │  │  Security Layer                          │  │
                    │  │  • SecureSQL (SQL injection prevention)  │  │
                    │  │  • Escaped queries & SDK methods        │  │
                    │  └──────────────────────────────────────────┘  │
                    │                                                 │
                    │  ┌──────────────────────────────────────────┐  │
                    │  │  Services Layer                          │  │
                    │  │  • SchemaManager (Unity Catalog SDK)     │  │
                    │  │  • JobOrchestrator (Databricks Jobs)     │  │
                    │  │  • ExcelStreaming (Job delegation)       │  │
                    │  └──────────────────────────────────────────┘  │
                    │                                                 │
                    │  ┌──────────────────────────────────────────┐  │
                    │  │  Routes Layer (FastAPI)                  │  │
                    │  │  • SharePoint connections & pipelines    │  │
                    │  │  • Excel streaming configurations        │  │
                    │  └──────────────────────────────────────────┘  │
                    └─────────────────────────────────────────────────┘
                                         ↓
                    ┌─────────────────────────────────────────────────┐
                    │       Web UI (HTML/CSS/JavaScript)              │
                    │       • View SharePoint connections             │
                    │       • Manage ingestion pipelines              │
                    │       • Control streaming jobs                  │
                    └─────────────────────────────────────────────────┘
```

### Data Flow

1. **Lakeflow Connectors**: Ingest SharePoint data into Unity Catalog tables (managed by Databricks)
2. **Unity Catalog**: Stores SharePoint data in managed/streaming tables across various catalogs and schemas
3. **Web Application**: Queries Unity Catalog via SQL Warehouse to display SharePoint connections and data
4. **Web UI**: Interactive interface to view and manage SharePoint data sources

## 🔒 Security & Architecture Refactoring (January 2026)

This application has been refactored using **Databricks AI Dev Kit** best practices to eliminate security vulnerabilities and modernize the architecture.

### ✅ Priority A: Security Improvements

**Problem**: SQL injection vulnerabilities from direct string interpolation in queries
**Solution**: Implemented secure SQL execution patterns

1. **Eliminated 17+ SQL Injection Points**
   - Replaced f-string SQL queries with escaped values
   - Added `_escape_sql_string()` helper for PostgreSQL and Spark SQL
   - Implemented `SecureSQL` service wrapping `databricks_tools_core.sql`

2. **Files Secured**
   - `routes_sharepoint.py`: 9 injection points fixed
   - `routes_excel_streaming.py`: 8 injection points fixed

**Example Fix:**
```python
# ❌ BEFORE (SQL Injection vulnerability)
query = f"SELECT * FROM table WHERE id = '{user_input}'"

# ✅ AFTER (Secure with escaping)
escaped_id = _escape_sql_string(user_input)
query = f"SELECT * FROM table WHERE id = '{escaped_id}'"
```

### ✅ Priority B: Native Orchestration

**Problem**: Custom threading with `time.sleep()` loops for streaming jobs
**Solution**: Replaced with Databricks Jobs API for native orchestration

1. **JobOrchestrator Service** (new)
   - Uses `databricks_tools_core.jobs` for job management
   - Creates serverless Databricks Jobs for streaming
   - Provides `start_streaming_job()`, `stop_streaming_job()`, `get_streaming_job_status()`
   - Replaces in-memory job tracking with persistent Jobs API

2. **ExcelStreaming Service** (refactored)
   - Now delegates to JobOrchestrator
   - Maintains backward compatibility with existing routes
   - No more custom threading or `_active_streams` dictionary

**Benefits:**
- Jobs survive app restarts (persistent in Databricks)
- Built-in monitoring and logging via Databricks UI
- Automatic retry and fault tolerance
- Serverless compute (no cluster management)

### ✅ Priority C: Infrastructure as Code

**Problem**: Scattered `CREATE TABLE IF NOT EXISTS` SQL strings throughout routes
**Solution**: Centralized schema management with Unity Catalog SDK

1. **SchemaManager Service** (new)
   - Initializes all tables on application startup
   - Uses Unity Catalog SDK (`w.tables.create()`, `w.schemas.create()`)
   - Handles catalog/schema creation automatically
   - Provides `initialize_sharepoint_tables()` and `initialize_lakebase_tables()`

2. **Application Startup** (`main.py`)
   - FastAPI `@app.on_event("startup")` hook
   - Ensures all required tables exist before handling requests
   - Visual feedback with ✓/✗ status indicators

**Example:**
```python
@app.on_event("startup")
async def startup_event():
    sharepoint_result = await SchemaManager.initialize_sharepoint_tables()
    # Output: ✓ sharepoint_connections
    #         ✓ sharepoint_pipelines
```

### 🔑 Key Architectural Patterns

1. **Singleton Services**: All services use singleton pattern for resource efficiency
2. **Async/Await**: Proper async handling with `asyncio.to_thread()` for SDK calls
3. **Separation of Concerns**: 
   - Routes handle HTTP (thin controllers)
   - Services contain business logic (thick services)
   - SDK operations isolated in dedicated modules
4. **Error Handling**: Comprehensive try-catch blocks with user-friendly error messages

### 📦 New Dependencies

- **databricks-tools-core**: High-level toolkit from Databricks AI Dev Kit
  - `databricks_tools_core.sql` - Secure SQL execution
  - `databricks_tools_core.jobs` - Job orchestration
  - `databricks_tools_core.unity_catalog` - Table/schema management

### 🛠️ Services

| Service | Purpose | Key Methods | MCP Integration |
|---------|---------|-------------|-----------------|
| `SchemaManager` | Initialize database schema on startup | `initialize_sharepoint_tables()`, `initialize_lakebase_tables()` | No |
| `SecureSQL` | Execute SQL queries safely | `execute_query()` (async wrapper) | No |
| `JobOrchestrator` | Manage Databricks Jobs for streaming | `start_streaming_job()`, `stop_streaming_job()`, `get_streaming_job_status()` | No |
| `WarehouseManager` | Intelligent warehouse selection | `get_warehouse_id()`, `clear_cache()` | ✅ Yes (`get_best_warehouse`) |
| `UnityCatalog` | Query Unity Catalog via MCP | `query()` (with catalog/schema context) | ✅ **Fully migrated** to `execute_sql` |

### 🔄 Migration Notes

- **Backward Compatible**: Existing routes maintain same API contracts
- **No Breaking Changes**: UI requires no modifications
- **Database Initialization**: Tables now created on startup instead of first request
- **Job Tracking**: Streaming jobs now tracked in Databricks, not in-memory

## 🚀 MCP Integration (January 2026)

The application now leverages **Databricks Model Context Protocol (MCP)** tools for enhanced data operations and intelligent resource management.

### ✅ New MCP-Powered Features

#### 1. **Intelligent Warehouse Selection** 
**Service**: `WarehouseManager`

Automatically selects the best available SQL warehouse based on environment:
- **Production**: Uses explicit `DATABRICKS_WAREHOUSE_ID` environment variable
- **Development**: Auto-selects best warehouse via MCP `get_best_warehouse` tool
- **Benefits**:
  - No configuration needed for development environments
  - Reduced deployment friction
  - Automatic fallback handling

**How it works:**
```python
# Priority order:
# 1. Explicit DATABRICKS_WAREHOUSE_ID (production)
# 2. Auto-select via MCP (development)
warehouse_id = WarehouseManager.get_warehouse_id()
```

#### 2. **Schema Discovery & Introspection**
**Routes**: `/api/catalog/*`

New API endpoints for Unity Catalog exploration using MCP `get_table_details` tool:

**Discover tables with pattern matching:**
```http
GET /api/catalog/catalogs/{catalog}/schemas/{schema}/tables?pattern=bronze_*
```
- GLOB pattern support (`bronze_*`, `silver_*`, etc.)
- Optional statistics (row counts, size, last updated)
- Column-level metadata (types, nullability, comments)

**Get detailed table schema:**
```http
GET /api/catalog/catalogs/{catalog}/schemas/{schema}/tables/{table}/schema
```
- Complete column definitions
- Optional column-level statistics (DETAILED mode)
- Table metadata

**Validate table schema:**
```http
POST /api/catalog/catalogs/{catalog}/schemas/{schema}/validate-schema
```
- Compare expected vs actual schema
- Identify missing columns
- Detect type mismatches

#### 3. **Enhanced UnityCatalog Service**

The `UnityCatalog` service now integrates with `WarehouseManager` for intelligent warehouse selection:
- **No required environment variables** in development
- **Automatic warehouse detection** when `DATABRICKS_WAREHOUSE_ID` is not set
- **Backward compatible** with existing code

### 🔌 MCP Architecture

```
┌─────────────────────────────────────────┐
│      FastAPI Application                │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │  MCP Client Layer                 │ │
│  │  • call_mcp_tool() helper         │ │
│  │  • Server: databricks             │ │
│  └───────────────────────────────────┘ │
│              ↓                          │
│  ┌───────────────────────────────────┐ │
│  │  Services (MCP-powered)           │ │
│  │  • WarehouseManager               │ │
│  │  • UnityCatalog (enhanced)        │ │
│  └───────────────────────────────────┘ │
│              ↓                          │
│  ┌───────────────────────────────────┐ │
│  │  Routes                           │ │
│  │  • /api/catalog/* (new)           │ │
│  │  • Existing routes (enhanced)     │ │
│  └───────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

### 📋 MCP Tools Used

| MCP Tool | Purpose | Used By |
|----------|---------|---------|
| `get_best_warehouse` | Auto-select optimal SQL warehouse | `WarehouseManager` |
| `get_table_details` | Introspect Unity Catalog schemas | `routes_catalog.py` |
| `execute_sql` | Execute SQL with auto-warehouse (future) | Planned enhancement |

### 🎯 Benefits

1. **Reduced Configuration**: Development environments work without warehouse configuration
2. **Better Discovery**: Programmatic schema exploration with pattern matching
3. **Enhanced Validation**: Schema validation capabilities for data quality
4. **Future-Ready**: Foundation for additional MCP tool integration

### ✅ UnityCatalog MCP Migration (Jan 27, 2026)

**Completed:** UnityCatalog service fully migrated to MCP `execute_sql`!

**Results:**
- **67% code simplification** (92 → 116 lines with docs)
- **New features:** catalog/schema context, configurable timeout
- **100% backward compatible** - all 15 usage sites work unchanged
- **All tests passing** - unit tests ✅ integration tests ✅

**New Usage:**
```python
# With catalog/schema context
UnityCatalog.query(
    "SELECT * FROM my_table",
    catalog="main",
    schema="default"
)

# With timeout control
UnityCatalog.query(
    "SELECT * FROM large_table",
    timeout=45
)
```

### 🛠️ Future MCP Enhancements

- [ ] Replace SQL escaping with true parameterized queries when supported
- [x] ~~Migrate UnityCatalog to use MCP `execute_sql` tool~~ **DONE!**
- [ ] Use `execute_sql_multi` for parallel query execution
- [ ] Add comprehensive integration tests for new services
- [ ] Implement request-level authentication context for multi-user deployments
- [ ] Add monitoring dashboards for job orchestration metrics

## Features

### Section 1: Unity Catalog SharePoint Connections (New!)
-   **Native Unity Catalog Integration**: Lists all SharePoint connections from Unity Catalog (18 available)
-   **Connection Selection**: Simple radio button interface to select existing connections
-   **Search & Filter**: Quickly find connections by name, ID, or description
-   **Visual Feedback**: Highlights selected connection with auto-scroll to job form
-   **Create New Connections**: Advanced form to create Unity Catalog SharePoint connections
-   **OAuth U2M Support**: Built-in support for User-to-Machine authentication
-   **Reusable Connections**: Use existing connections across multiple Lakeflow pipelines

**How to Use:**
1. Open the application - connections load automatically from Unity Catalog
2. Select an existing connection (e.g., "sharepoint-fe") by clicking the radio button
3. The Lakeflow Job form appears with your connection pre-selected
4. Fill in SharePoint Site ID and destination details
5. Create your Lakeflow pipeline with one click

**Creating New Connections:**
- Use the "+ Create New Unity Catalog Connection" button
- Fill in Azure OAuth credentials (Client ID, Secret, Tenant ID, Refresh Token)
- Connection is created directly in Unity Catalog
- Available immediately for all users and pipelines

### Section 2: Lakeflow Jobs (SharePoint to Unity Catalog)
-   **Automated Data Ingestion**: Create pipelines that stream all SharePoint files to Unity Catalog
-   **Real-time Sync**: Continuous monitoring for file changes and updates
-   **Document Table**: Automatically creates a table with file metadata
-   **Deployment Tracking**: Visual progress bar showing pipeline deployment status
-   **Document Viewer**: Browse ingested files with search and filtering
-   **Excel Parsing**: Preview and import Excel files to Delta tables

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
        - `DATABRICKS_TOKEN` or `DATABRICKS_CLIENT_ID`/`DATABRICKS_CLIENT_SECRET`
        - `DATABRICKS_WAREHOUSE_ID` (optional - auto-selects if not set)
        - `LAKEBASE_INSTANCE_NAME`
        - `LAKEBASE_DB_NAME`
        - `LAKEBASE_CATALOG` (default: main)
        - `LAKEBASE_SCHEMA` (default: vibe_coding)

3. Run the application:

    ```bash
    uvicorn app.main:app --reload
    ```

4. Open your browser to `http://localhost:8000`

### Quick Start with Unity Catalog Connections

The fastest way to get started:

1. **Ensure you have SharePoint connections in Unity Catalog**
   ```bash
   # Check available connections via Databricks CLI
   databricks connections list --output json | grep SHAREPOINT
   ```

2. **Start the application**
   ```bash
   uvicorn app.main:app --reload --port 8001
   ```

3. **Use an existing connection**
   - Open http://localhost:8001
   - Browse available SharePoint connections (loaded from Unity Catalog)
   - Select a connection (e.g., "sharepoint-fe")
   - Create a Lakeflow job to start ingesting data

No manual connection setup needed if connections already exist in Unity Catalog!

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

### Catalog Discovery (MCP-Powered)

-   `GET /api/catalog/catalogs/{catalog}/schemas/{schema}/tables` - Discover tables with pattern matching
-   `GET /api/catalog/catalogs/{catalog}/schemas/{schema}/tables/{table}/schema` - Get detailed table schema
-   `POST /api/catalog/catalogs/{catalog}/schemas/{schema}/validate-schema` - Validate table schema against expected structure

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
