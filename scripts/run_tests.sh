#!/bin/bash
# Test execution script with coverage reporting

set -e

echo "============================================"
echo "Running Test Suite with Coverage"
echo "============================================"
echo ""

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo "Error: pytest not installed"
    echo "Run: pip install -r requirements.txt"
    exit 1
fi

# Create coverage directory
mkdir -p docs/coverage_reports

echo "Running tests with coverage..."
echo ""

# Run tests with coverage
pytest tests/ \
    --cov=app \
    --cov-report=html:docs/coverage_reports/html \
    --cov-report=term \
    --cov-report=json:docs/coverage_reports/coverage.json \
    -v \
    --tb=short

echo ""
echo "============================================"
echo "Coverage Report Generated"
echo "============================================"
echo "HTML Report: docs/coverage_reports/html/index.html"
echo "JSON Report: docs/coverage_reports/coverage.json"
echo ""

# Generate dead code analysis
echo "============================================"
echo "Analyzing Dead Code"
echo "============================================"
echo ""

python3 scripts/analyze_coverage.py

echo ""
echo "============================================"
echo "Test Suite Complete!"
echo "============================================"
