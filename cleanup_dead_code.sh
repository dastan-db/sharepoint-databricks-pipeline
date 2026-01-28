#!/bin/bash
# Dead Code Cleanup Script
# Removes legacy Lakebase-related code identified by backward testing

set -e

echo "============================================"
echo "Dead Code Cleanup"
echo "============================================"
echo ""
echo "This script will remove legacy Lakebase code identified by testing."
echo "The following files will be deleted:"
echo ""
echo "Application Files:"
echo "  - app/services/lakebase.py"
echo "  - app/services/data_quality.py"
echo "  - app/services/excel_parser.py"
echo "  - app/services/update_checker.py"
echo "  - app/api/routes_config.py"
echo "  - app/api/routes_runs.py"
echo "  - app/core/pipeline.py"
echo ""
echo "Test Files:"
echo "  - tests/services/test_lakebase.py"
echo "  - tests/services/test_data_quality.py"
echo "  - tests/services/test_excel_parser.py"
echo "  - tests/services/test_update_checker.py"
echo "  - tests/api/test_routes_config.py"
echo "  - tests/api/test_routes_runs.py"
echo "  - tests/core/test_pipeline.py"
echo ""
echo "Total: ~400-500 lines of dead code"
echo ""
read -p "Continue with cleanup? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cleanup cancelled."
    exit 0
fi

echo ""
echo "Removing dead code files..."

# Remove application files
rm -f app/services/lakebase.py
rm -f app/services/data_quality.py
rm -f app/services/excel_parser.py
rm -f app/services/update_checker.py
rm -f app/api/routes_config.py
rm -f app/api/routes_runs.py
rm -f app/core/pipeline.py

echo "✓ Removed 7 application files"

# Remove test files
rm -f tests/services/test_lakebase.py
rm -f tests/services/test_data_quality.py
rm -f tests/services/test_excel_parser.py
rm -f tests/services/test_update_checker.py
rm -f tests/api/test_routes_config.py
rm -f tests/api/test_routes_runs.py
rm -f tests/core/test_pipeline.py

echo "✓ Removed 7 test files"

echo ""
echo "============================================"
echo "Manual Steps Required"
echo "============================================"
echo ""
echo "1. Update app/main.py:"
echo "   - Remove imports: routes_runs, routes_config"
echo "   - Remove router registrations for /runs and /configs"
echo "   - Simplify startup_event() - remove empty method calls"
echo ""
echo "2. Update app/services/schema_manager.py:"
echo "   - Remove initialize_sharepoint_tables() method"
echo "   - Remove initialize_lakebase_tables() method"
echo ""
echo "3. Update requirements.txt:"
echo "   - Remove: psycopg2-binary (only used by Lakebase)"
echo ""
echo "4. Re-run tests:"
echo "   pytest tests/ -v"
echo ""
echo "============================================"
echo "Cleanup Complete!"
echo "============================================"
