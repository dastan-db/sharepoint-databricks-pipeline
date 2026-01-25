# app/services/excel_parser.py
from typing import Dict, Any
import io
import pandas as pd
from app.core.models import SyncConfig
from app.services.lakebase import Lakebase


def parse_excel_to_delta(config: SyncConfig) -> Dict[str, Any]:
    """
    Read an Excel file from the documents table and write/overwrite a Delta table.
    Returns a dict with status and metadata.
    """
    target_fq = f"{config.catalog}.{config.schema_name}.{config.target_table}"
    docs_table_fq = f"{config.catalog}.{config.schema_name}.{config.documents_table}"

    try:
        # Read Excel file from documents table
        query = f"""
            SELECT
              file_metadata.name as filename,
              content.inline_content as file_content,
              file_metadata.last_modified_timestamp as last_modified
            FROM {docs_table_fq}
            WHERE file_metadata.name = '{config.file_name}'
        """
        excel_rows = Lakebase.query(query)

        if not excel_rows:
            error_msg = f"Excel file {config.file_name} not found in {docs_table_fq}"
            return {
                "status": "error",
                "message": error_msg,
                "rows_processed": 0,
            }

        file_content = excel_rows[0][1]  # content.inline_content
        last_modified = excel_rows[0][2]  # last_modified_timestamp

    except Exception as e:
        return {
            "status": "error",
            "message": f"Error reading Excel file: {str(e)}",
            "rows_processed": 0,
        }

    try:
        # Read full Excel to get Supplier ID from B1
        full_data = pd.read_excel(io.BytesIO(file_content), sheet_name="Sheet1", header=None)
        supplier_id = full_data.iloc[0, 1]

        # Read Excel with headers from row 3
        data_df = pd.read_excel(io.BytesIO(file_content), sheet_name="Sheet1", header=2)

        # Delete existing data
        Lakebase.query(f"DELETE FROM {target_fq}")

        if len(data_df) == 0:
            return {
                "status": "success",
                "message": "Empty table (cleared existing data)",
                "rows_processed": 0,
                "supplier_id": str(supplier_id),
            }

        data_df["supplier_id"] = supplier_id
        data_df["last_modified"] = last_modified

        # Insert rows one by one
        for _, row in data_df.iterrows():
            # Escape single quotes in values
            values = []
            for val in row.values:
                if pd.isna(val):
                    values.append("NULL")
                elif isinstance(val, str):
                    escaped_val = str(val).replace("'", "''")
                    values.append(f"'{escaped_val}'")
                else:
                    values.append(f"'{val}'")
            
            values_str = ", ".join(values)
            insert_query = f"""
                INSERT INTO {target_fq} VALUES ({values_str}) RETURNING *
            """
            Lakebase.query(insert_query)

        return {
            "status": "success",
            "message": "Table updated successfully",
            "rows_processed": len(data_df),
            "supplier_id": str(supplier_id),
            "columns": list(data_df.columns),
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Error parsing Excel: {str(e)}",
            "rows_processed": 0,
        }
