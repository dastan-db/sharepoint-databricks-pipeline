# app/services/data_quality.py
from typing import Dict, Any, List
from app.core.models import SyncConfig
from app.services.lakebase import Lakebase


def run_data_quality_checks(
    config: SyncConfig,
    required_columns: List[str] | None = None,
) -> Dict[str, Any]:
    """
    Run basic data quality checks on the target table.
    """
    if required_columns is None:
        required_columns = ["Date", "SKU", "Qty", "supplier_id"]

    table_name = f"{config.catalog}.{config.schema_name}.{config.target_table}"

    try:
        # Get row count
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        count_rows = Lakebase.query(count_query)
        row_count = count_rows[0][0] if count_rows else 0
    except Exception as e:
        return {
            "status": "error",
            "message": f"Table not found: {str(e)}",
            "checks_passed": False,
        }

    quality_checks: List[Dict[str, Any]] = []

    # Check 1: Row count
    check_1 = {
        "check": "row_count",
        "passed": row_count > 0,
        "value": row_count,
        "message": f"Table has {row_count} rows",
    }
    quality_checks.append(check_1)

    # Check 2: Required columns (get from information schema)
    try:
        columns_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{config.schema_name}' 
              AND table_name = '{config.target_table}'
        """
        column_rows = Lakebase.query(columns_query)
        actual_columns = [row[0] for row in column_rows]
        
        missing_columns = [c for c in required_columns if c not in actual_columns]
        check_2 = {
            "check": "required_columns",
            "passed": len(missing_columns) == 0,
            "value": actual_columns,
            "message": (
                f"Missing columns: {missing_columns}"
                if missing_columns
                else "All required columns present"
            ),
        }
        quality_checks.append(check_2)
    except Exception as e:
        check_2 = {
            "check": "required_columns",
            "passed": False,
            "value": [],
            "message": f"Error checking columns: {str(e)}",
        }
        quality_checks.append(check_2)

    # Check 3: Nulls in key columns
    if row_count > 0:
        try:
            null_counts = {}
            for col in required_columns:
                if col in actual_columns:
                    null_query = f"""
                        SELECT COUNT(*) 
                        FROM {table_name} 
                        WHERE "{col}" IS NULL
                    """
                    null_rows = Lakebase.query(null_query)
                    null_counts[col] = null_rows[0][0] if null_rows else 0
            
            has_nulls = any(v > 0 for v in null_counts.values())
            check_3 = {
                "check": "null_values",
                "passed": not has_nulls,
                "value": null_counts,
                "message": f"Null counts: {null_counts}",
            }
            quality_checks.append(check_3)
        except Exception as e:
            check_3 = {
                "check": "null_values",
                "passed": False,
                "value": {},
                "message": f"Error checking nulls: {str(e)}",
            }
            quality_checks.append(check_3)

    # Check 4: Supplier ID consistency
    if row_count > 0 and "supplier_id" in actual_columns:
        try:
            distinct_query = f"""
                SELECT COUNT(DISTINCT supplier_id) 
                FROM {table_name}
            """
            distinct_rows = Lakebase.query(distinct_query)
            distinct_suppliers = distinct_rows[0][0] if distinct_rows else 0
            
            check_4 = {
                "check": "supplier_consistency",
                "passed": distinct_suppliers == 1,
                "value": distinct_suppliers,
                "message": f"Found {distinct_suppliers} distinct supplier(s)",
            }
            quality_checks.append(check_4)
        except Exception as e:
            check_4 = {
                "check": "supplier_consistency",
                "passed": False,
                "value": 0,
                "message": f"Error checking supplier consistency: {str(e)}",
            }
            quality_checks.append(check_4)

    all_passed = all(c["passed"] for c in quality_checks)
    total_checks = len(quality_checks)
    passed_checks = sum(1 for c in quality_checks if c["passed"])

    return {
        "status": "success",
        "checks_passed": all_passed,
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": total_checks - passed_checks,
        "quality_checks": quality_checks,
        "row_count": row_count,
    }
