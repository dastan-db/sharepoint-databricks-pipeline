# app/api/routes_excel.py
from fastapi import APIRouter, HTTPException
from databricks.sdk import WorkspaceClient
from app.services.unity_catalog import UnityCatalog
import pandas as pd
import io
import os
import re
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

router = APIRouter()


class ParseExcelRequest(BaseModel):
    connection_id: str
    file_path: str
    table_name: str
    sheet_name: Optional[str] = None
    header_row: int = 0  # Which row contains headers (0-indexed)
    selected_columns: Optional[List[str]] = None  # Only include these columns
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
    file_path: str,
    max_rows: int = 100
):
    """
    Read Excel file from documents table and return raw data for interactive column selection.
    
    Returns:
    - sheets: List of sheet names
    - raw_data: All rows as lists (no headers assumed)
    - total_rows: Number of rows in preview
    - column_count: Number of columns
    - recommended_table_name: Suggested table name
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
        excel_file = pd.ExcelFile(io.BytesIO(file_content), engine='openpyxl')
        sheets = excel_file.sheet_names
        
        # Read WITHOUT headers first (header=None) for raw display
        df = pd.read_excel(excel_file, sheet_name=sheets[0], header=None, nrows=max_rows)
        
        # Convert to list of lists (raw data)
        raw_data = []
        for _, row in df.iterrows():
            row_data = []
            for value in row:
                if pd.isna(value):
                    row_data.append(None)
                elif isinstance(value, (pd.Timestamp, datetime)):
                    row_data.append(str(value))
                else:
                    row_data.append(value)
            raw_data.append(row_data)
        
        return {
            "file_path": file_path,
            "sheets": sheets,
            "selected_sheet": sheets[0],
            "raw_data": raw_data,
            "total_rows": len(df),
            "column_count": len(df.columns) if len(df) > 0 else 0,
            "recommended_table_name": _generate_table_name(file_path)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to preview Excel: {str(e)}")


@router.get("/analyze-columns")
async def analyze_columns_with_header(
    connection_id: str,
    file_path: str,
    header_row: int = 0,
    sheet_name: Optional[str] = None
):
    """
    Analyze Excel columns with specified header row.
    Returns column names, types, and sample data for interactive column selection.
    
    Parameters:
    - connection_id: Lakeflow job connection ID
    - file_path: Path to file in documents table
    - header_row: Which row to use as headers (0-indexed)
    - sheet_name: Optional sheet name (defaults to first sheet)
    
    Returns:
    - columns: List of column metadata (name, type, nullable, sample_values)
    - row_count: Total number of data rows (excluding header)
    - header_row: Confirmed header row index
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
        file_query = f"""
            SELECT content 
            FROM {doc_table} 
            WHERE file_id = '{file_path}' AND is_deleted = false
            LIMIT 1
        """
        file_rows = UnityCatalog.query(file_query)
        
        if not file_rows:
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
        
        file_content = file_rows[0]['content']
        
        # Convert to bytes if needed
        if isinstance(file_content, str):
            import base64
            file_content = base64.b64decode(file_content)
        
        # 3. Parse Excel with specified header row
        df = pd.read_excel(
            io.BytesIO(file_content),
            sheet_name=sheet_name or 0,
            header=header_row,
            engine='openpyxl'
        )
        
        # 4. Analyze columns
        columns = []
        for col in df.columns:
            dtype = df[col].dtype
            spark_type = _pandas_to_spark_type(dtype)
            
            # Get sample values (first 3 non-null values)
            sample_values = []
            for val in df[col].dropna().head(3):
                if isinstance(val, (pd.Timestamp, datetime)):
                    sample_values.append(str(val))
                else:
                    sample_values.append(val)
            
            columns.append({
                "name": str(col),
                "type": spark_type,
                "nullable": bool(df[col].isnull().any()),
                "sample_values": sample_values
            })
        
        return {
            "columns": columns,
            "row_count": len(df),
            "header_row": header_row
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to analyze columns: {str(e)}")


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
        
        # Parse Excel with specified header row
        df = pd.read_excel(
            io.BytesIO(file_content),
            sheet_name=request.sheet_name or 0,
            header=request.header_row,
            engine='openpyxl'
        )
        
        # Filter to selected columns only
        if request.selected_columns:
            # Ensure selected columns exist in dataframe
            available_cols = [col for col in request.selected_columns if col in df.columns]
            if not available_cols:
                raise HTTPException(
                    status_code=400, 
                    detail=f"None of the selected columns found in Excel file"
                )
            df = df[available_cols]
        
        # 3. Prepare schema
        if request.schema is None:
            # Auto-detect schema from filtered dataframe
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
                elif isinstance(value, (pd.Timestamp, datetime)):
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
