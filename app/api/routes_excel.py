# app/api/routes_excel.py
from fastapi import APIRouter, HTTPException
from databricks.sdk import WorkspaceClient
from app.services.unity_catalog import UnityCatalog
import pandas as pd
import io
import os
import re
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

router = APIRouter()


class ParseExcelRequest(BaseModel):
    connection_id: str
    file_path: str
    table_name: str
    sheet_name: Optional[str] = None
    schema: Optional[List[Dict[str, str]]] = None


def _get_lakeflow_jobs_table():
    """Get fully qualified table name for lakeflow jobs"""
    catalog = os.getenv("UC_CATALOG", "main")
    schema = os.getenv("SHAREPOINT_SCHEMA_PREFIX", "sharepoint")
    return f"{catalog}.{schema}.lakeflow_jobs"


def _pandas_to_spark_type(dtype):
    """Map pandas dtype to Spark SQL type."""
    dtype_str = str(dtype)
    if 'int' in dtype_str:
        return 'BIGINT'
    elif 'float' in dtype_str:
        return 'DOUBLE'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    elif 'datetime' in dtype_str:
        return 'TIMESTAMP'
    else:
        return 'STRING'


def _generate_table_name(file_path: str) -> str:
    """Generate safe table name from file path."""
    name = file_path.split('/')[-1]
    # Remove extension
    name = re.sub(r'\.(xlsx|xls)$', '', name, flags=re.IGNORECASE)
    # Replace special chars with underscores
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    # Remove leading/trailing underscores and convert to lowercase
    return name.strip('_').lower()


@router.get("/preview")
async def preview_excel_file(
    connection_id: str,
    file_path: str
):
    """
    Read Excel file from documents table and auto-detect schema.
    
    Returns:
    - sheets: List of sheet names
    - columns: Detected columns with inferred types
    - sample_rows: First 5 rows as preview
    - recommended_schema: Suggested Delta table schema
    """
    try:
        # 1. Get document table name from lakeflow_jobs
        jobs_table = _get_lakeflow_jobs_table()
        query = f"""
            SELECT document_table 
            FROM {jobs_table} 
            WHERE connection_id = '{connection_id}'
        """
        rows = UnityCatalog.query(query)
        
        if not rows:
            raise HTTPException(status_code=404, detail="Job not found")
        
        doc_table = rows[0]['document_table']
        
        # 2. Read file content from documents table
        # SharePoint connector schema: file_id, file_metadata (object), content (binary), is_deleted
        file_query = f"""
            SELECT content 
            FROM {doc_table} 
            WHERE file_id = '{file_path}' AND is_deleted = false
            LIMIT 1
        """
        file_rows = UnityCatalog.query(file_query)
        
        if not file_rows:
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
        
        # Get binary content
        file_content = file_rows[0]['content']
        
        # Convert to bytes if needed
        if isinstance(file_content, str):
            # If it's a string, it might be base64 encoded
            import base64
            file_content = base64.b64decode(file_content)
        
        # 3. Parse Excel with pandas
        excel_file = pd.ExcelFile(io.BytesIO(file_content))
        sheets = excel_file.sheet_names
        
        # Read first sheet for preview (limited rows)
        df = pd.read_excel(excel_file, sheet_name=sheets[0], nrows=5)
        
        # 4. Auto-detect schema
        detected_columns = []
        for col in df.columns:
            dtype = df[col].dtype
            spark_type = _pandas_to_spark_type(dtype)
            detected_columns.append({
                "name": str(col),
                "type": spark_type,
                "nullable": bool(df[col].isnull().any())
            })
        
        # 5. Sample rows
        sample_rows = df.to_dict('records')
        # Convert any non-serializable types
        for row in sample_rows:
            for key, value in row.items():
                if pd.isna(value):
                    row[key] = None
                elif isinstance(value, (pd.Timestamp, pd.datetime)):
                    row[key] = str(value)
        
        return {
            "file_path": file_path,
            "sheets": sheets,
            "selected_sheet": sheets[0],
            "columns": detected_columns,
            "sample_rows": sample_rows,
            "row_count_preview": len(df),
            "recommended_table_name": _generate_table_name(file_path)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to preview Excel: {str(e)}")


@router.post("/parse")
async def parse_excel_to_delta(request: ParseExcelRequest):
    """
    Parse Excel file and create Delta table with auto-detected or custom schema.
    
    Uses Databricks SQL to:
    1. Read content from documents table
    2. Parse Excel rows
    3. Create Delta table with specified schema
    4. Insert data
    """
    try:
        # 1. Get document table and destination info
        jobs_table = _get_lakeflow_jobs_table()
        query = f"""
            SELECT document_table, destination_catalog, destination_schema 
            FROM {jobs_table} 
            WHERE connection_id = '{request.connection_id}'
        """
        rows = UnityCatalog.query(query)
        
        if not rows:
            raise HTTPException(status_code=404, detail="Job not found")
        
        doc_table = rows[0]['document_table']
        catalog = rows[0]['destination_catalog']
        schema_name = rows[0]['destination_schema']
        
        # 2. Read and parse Excel file
        file_query = f"""
            SELECT content 
            FROM {doc_table} 
            WHERE file_id = '{request.file_path}' AND is_deleted = false
            LIMIT 1
        """
        file_rows = UnityCatalog.query(file_query)
        
        if not file_rows:
            raise HTTPException(status_code=404, detail=f"File not found: {request.file_path}")
        
        file_content = file_rows[0]['content']
        
        # Convert to bytes if needed
        if isinstance(file_content, str):
            import base64
            file_content = base64.b64decode(file_content)
        
        # Parse Excel
        df = pd.read_excel(
            io.BytesIO(file_content),
            sheet_name=request.sheet_name or 0
        )
        
        # 3. Prepare schema
        if request.schema is None:
            # Auto-detect schema
            schema = []
            for col in df.columns:
                spark_type = _pandas_to_spark_type(df[col].dtype)
                schema.append({"name": str(col), "type": spark_type})
        else:
            schema = request.schema
        
        # 4. Create Delta table
        full_table_name = f"{catalog}.{schema_name}.{request.table_name}"
        
        # Build CREATE TABLE statement
        columns_def = ", ".join([f"`{c['name']}` {c['type']}" for c in schema])
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {columns_def}
            ) USING DELTA
        """
        UnityCatalog.query(create_query)
        
        # 5. Insert data row by row
        # Note: This is simplified. For large files, consider using Spark DataFrame write
        rows_inserted = 0
        for _, row in df.iterrows():
            # Build INSERT statement
            values = []
            for col in schema:
                col_name = col['name']
                value = row[col_name]
                
                if pd.isna(value):
                    values.append('NULL')
                elif isinstance(value, str):
                    # Escape single quotes
                    escaped = value.replace("'", "''")
                    values.append(f"'{escaped}'")
                elif isinstance(value, (pd.Timestamp, pd.datetime)):
                    values.append(f"TIMESTAMP '{value}'")
                else:
                    values.append(str(value))
            
            values_str = ", ".join(values)
            insert_query = f"INSERT INTO {full_table_name} VALUES ({values_str})"
            
            try:
                UnityCatalog.query(insert_query)
                rows_inserted += 1
            except Exception as e:
                print(f"Failed to insert row: {e}")
                # Continue with other rows
        
        return {
            "message": "Excel parsed successfully",
            "table_name": full_table_name,
            "rows_inserted": rows_inserted,
            "total_rows": len(df),
            "columns": schema
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse Excel: {str(e)}")
