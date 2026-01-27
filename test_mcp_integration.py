#!/usr/bin/env python3
"""
Test script for MCP integration.
Run this to validate that MCP client and services are working correctly.
"""
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Check required environment variables
required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
    print("Please set these in your .env file")
    sys.exit(1)

print("=" * 60)
print("MCP Integration Test Suite")
print("=" * 60)
print()

# Test 1: MCP Client - get_best_warehouse
print("Test 1: get_best_warehouse")
print("-" * 60)
try:
    from app.core.mcp_client import call_mcp_tool
    
    result = call_mcp_tool(
        server="project-0-fe-vibe-app-databricks",
        tool_name="get_best_warehouse",
        arguments={}
    )
    
    warehouse_id = result.get("result")
    if warehouse_id:
        print(f"✅ Success! Best warehouse: {warehouse_id}")
    else:
        print("⚠️  No warehouses available in workspace")
except Exception as e:
    print(f"❌ Failed: {e}")

print()

# Test 2: WarehouseManager Service
print("Test 2: WarehouseManager Service")
print("-" * 60)
try:
    from app.services.warehouse_manager import WarehouseManager
    
    # Test with env var (if set)
    warehouse_id = WarehouseManager.get_warehouse_id()
    if warehouse_id:
        source = "DATABRICKS_WAREHOUSE_ID env var" if os.getenv("DATABRICKS_WAREHOUSE_ID") else "MCP auto-selection"
        print(f"✅ Success! Warehouse ID: {warehouse_id}")
        print(f"   Source: {source}")
    else:
        print("⚠️  No warehouse available")
    
    # Test auto-selection
    WarehouseManager.clear_cache()
    warehouse_id = WarehouseManager.get_warehouse_id(force_auto_select=True)
    if warehouse_id:
        print(f"✅ Auto-selection works! Warehouse ID: {warehouse_id}")
    else:
        print("⚠️  Auto-selection found no warehouses")
        
except Exception as e:
    print(f"❌ Failed: {e}")

print()

# Test 3: get_table_details (requires catalog/schema)
print("Test 3: get_table_details")
print("-" * 60)
try:
    # Try to list tables in main.default (common catalog/schema)
    result = call_mcp_tool(
        server="project-0-fe-vibe-app-databricks",
        tool_name="get_table_details",
        arguments={
            "catalog": "main",
            "schema": "default",
            "table_stat_level": "NONE"
        }
    )
    
    tables = result.get("tables", [])
    print(f"✅ Success! Found {len(tables)} tables in main.default")
    
    if tables:
        print(f"   Sample tables:")
        for table in tables[:3]:
            col_count = len(table.get("columns", []))
            print(f"   - {table['name']} ({col_count} columns)")
    else:
        print("   (No tables in main.default schema)")
        
except Exception as e:
    print(f"⚠️  Could not list tables: {e}")
    print("   This is okay if main.default doesn't exist in your workspace")

print()

# Test 4: execute_sql
print("Test 4: execute_sql")
print("-" * 60)
try:
    result = call_mcp_tool(
        server="project-0-fe-vibe-app-databricks",
        tool_name="execute_sql",
        arguments={
            "sql_query": "SELECT 'Hello from MCP!' as message, 42 as answer"
        }
    )
    
    rows = result.get("result", [])
    if rows:
        print(f"✅ Success! Query returned {len(rows)} row(s)")
        print(f"   Result: {rows[0]}")
    else:
        print("⚠️  Query executed but returned no rows")
        
except Exception as e:
    print(f"❌ Failed: {e}")

print()

# Test 5: UnityCatalog Service Integration
print("Test 5: UnityCatalog Service Integration")
print("-" * 60)
try:
    from app.services.unity_catalog import UnityCatalog
    
    # Simple query to test integration
    result = UnityCatalog.query("SELECT 1 as test")
    
    if result:
        print(f"✅ Success! UnityCatalog.query() works")
        print(f"   Using WarehouseManager for warehouse selection")
        print(f"   Result: {result}")
    else:
        print("⚠️  Query executed but returned no rows")
        
except Exception as e:
    print(f"❌ Failed: {e}")

print()
print("=" * 60)
print("MCP Integration Test Complete")
print("=" * 60)
print()

# Summary
print("Summary:")
print("- MCP Client implements 3 tools: get_best_warehouse, get_table_details, execute_sql")
print("- WarehouseManager uses MCP for auto-selection in dev environments")
print("- UnityCatalog service enhanced with intelligent warehouse selection")
print("- Catalog routes (/api/catalog/*) provide schema discovery APIs")
print()
print("Next steps:")
print("1. Test the catalog routes via FastAPI: uvicorn app.main:app --reload")
print("2. Try the /docs endpoint for interactive API testing")
print("3. Use /api/catalog endpoints to discover tables in your workspace")
