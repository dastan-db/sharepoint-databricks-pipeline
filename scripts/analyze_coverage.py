#!/usr/bin/env python3
"""
Coverage analysis script to identify dead code and unused components.
Analyzes test coverage and generates a report of untested code.
"""
import json
import os
from pathlib import Path
from typing import Dict, List, Set


def load_coverage_data() -> Dict:
    """Load coverage JSON data."""
    coverage_path = Path("coverage_reports/coverage.json")
    
    if not coverage_path.exists():
        print("Error: Coverage report not found. Run tests first.")
        return {}
    
    with open(coverage_path, 'r') as f:
        return json.load(f)


def analyze_coverage(coverage_data: Dict) -> Dict:
    """Analyze coverage data to identify dead code."""
    files = coverage_data.get("files", {})
    
    analysis = {
        "total_files": 0,
        "tested_files": 0,
        "untested_files": [],
        "low_coverage_files": [],  # < 50% coverage
        "medium_coverage_files": [],  # 50-80% coverage
        "high_coverage_files": [],  # > 80% coverage
        "completely_untested_lines": {},
        "overall_coverage": 0.0
    }
    
    for filepath, file_data in files.items():
        # Skip test files themselves
        if "tests/" in filepath or "test_" in filepath:
            continue
        
        # Skip __init__ files
        if "__init__.py" in filepath:
            continue
        
        analysis["total_files"] += 1
        
        summary = file_data.get("summary", {})
        covered_lines = summary.get("covered_lines", 0)
        num_statements = summary.get("num_statements", 0)
        
        if num_statements == 0:
            continue
        
        coverage_pct = (covered_lines / num_statements * 100) if num_statements > 0 else 0
        
        if covered_lines > 0:
            analysis["tested_files"] += 1
        else:
            analysis["untested_files"].append(filepath)
        
        if coverage_pct == 0:
            pass  # Already in untested_files
        elif coverage_pct < 50:
            analysis["low_coverage_files"].append((filepath, coverage_pct))
        elif coverage_pct < 80:
            analysis["medium_coverage_files"].append((filepath, coverage_pct))
        else:
            analysis["high_coverage_files"].append((filepath, coverage_pct))
        
        # Find completely untested lines
        missing_lines = file_data.get("missing_lines", [])
        if missing_lines:
            analysis["completely_untested_lines"][filepath] = missing_lines
    
    # Calculate overall coverage
    totals = coverage_data.get("totals", {})
    analysis["overall_coverage"] = totals.get("percent_covered", 0.0)
    
    return analysis


def identify_unused_imports() -> List[str]:
    """Identify potentially unused imports by checking if they're tested."""
    unused_candidates = []
    
    # Check for services that might be unused
    app_path = Path("app")
    
    if app_path.exists():
        # Check services directory
        services_path = app_path / "services"
        if services_path.exists():
            for service_file in services_path.glob("*.py"):
                if service_file.name == "__init__.py":
                    continue
                
                # Check if there's a corresponding test file
                test_file = Path(f"tests/services/test_{service_file.name}")
                if not test_file.exists():
                    unused_candidates.append(f"services/{service_file.name}")
    
    return unused_candidates


def generate_report(analysis: Dict, unused_imports: List[str]):
    """Generate and print the coverage analysis report."""
    print("=" * 80)
    print("COVERAGE ANALYSIS REPORT")
    print("=" * 80)
    print()
    
    print(f"Overall Coverage: {analysis['overall_coverage']:.2f}%")
    print(f"Total Application Files: {analysis['total_files']}")
    print(f"Files with Tests: {analysis['tested_files']}")
    print(f"Files without Tests: {len(analysis['untested_files'])}")
    print()
    
    print("=" * 80)
    print("COVERAGE DISTRIBUTION")
    print("=" * 80)
    print(f"High Coverage (>80%): {len(analysis['high_coverage_files'])} files")
    print(f"Medium Coverage (50-80%): {len(analysis['medium_coverage_files'])} files")
    print(f"Low Coverage (<50%): {len(analysis['low_coverage_files'])} files")
    print(f"No Coverage (0%): {len(analysis['untested_files'])} files")
    print()
    
    if analysis['untested_files']:
        print("=" * 80)
        print("COMPLETELY UNTESTED FILES (DEAD CODE CANDIDATES)")
        print("=" * 80)
        for filepath in analysis['untested_files']:
            print(f"  ❌ {filepath}")
        print()
    
    if analysis['low_coverage_files']:
        print("=" * 80)
        print("LOW COVERAGE FILES (<50%)")
        print("=" * 80)
        for filepath, coverage in sorted(analysis['low_coverage_files'], key=lambda x: x[1]):
            print(f"  ⚠️  {filepath}: {coverage:.1f}%")
        print()
    
    if analysis['medium_coverage_files']:
        print("=" * 80)
        print("MEDIUM COVERAGE FILES (50-80%)")
        print("=" * 80)
        for filepath, coverage in sorted(analysis['medium_coverage_files'], key=lambda x: x[1]):
            print(f"  ⚡ {filepath}: {coverage:.1f}%")
        print()
    
    if analysis['high_coverage_files']:
        print("=" * 80)
        print("HIGH COVERAGE FILES (>80%)")
        print("=" * 80)
        for filepath, coverage in sorted(analysis['high_coverage_files'], key=lambda x: x[1], reverse=True):
            print(f"  ✅ {filepath}: {coverage:.1f}%")
        print()
    
    if unused_imports:
        print("=" * 80)
        print("POTENTIALLY UNUSED MODULES (No Test Files)")
        print("=" * 80)
        for module in unused_imports:
            print(f"  🔍 {module}")
        print()
    
    # Recommendations
    print("=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print()
    
    if analysis['untested_files']:
        print("1. Review completely untested files for removal or add tests")
        print(f"   - {len(analysis['untested_files'])} files with 0% coverage")
        print()
    
    if analysis['low_coverage_files']:
        print("2. Improve coverage for low-coverage files")
        print(f"   - {len(analysis['low_coverage_files'])} files with <50% coverage")
        print()
    
    if unused_imports:
        print("3. Review modules without corresponding test files")
        print(f"   - {len(unused_imports)} modules may be unused")
        print()
    
    print("4. Review untested code paths in:")
    print("   - Error handling branches")
    print("   - Edge case conditions")
    print("   - Optional feature code")
    print()
    
    print("=" * 80)


def main():
    """Main analysis function."""
    # Load coverage data
    coverage_data = load_coverage_data()
    
    if not coverage_data:
        return
    
    # Analyze coverage
    analysis = analyze_coverage(coverage_data)
    
    # Identify unused imports
    unused_imports = identify_unused_imports()
    
    # Generate report
    generate_report(analysis, unused_imports)
    
    # Save detailed report to file
    report_path = Path("coverage_reports/dead_code_analysis.txt")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_path, 'w') as f:
        f.write("DEAD CODE ANALYSIS\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Overall Coverage: {analysis['overall_coverage']:.2f}%\n\n")
        
        f.write("UNTESTED FILES:\n")
        for filepath in analysis['untested_files']:
            f.write(f"  - {filepath}\n")
        f.write("\n")
        
        f.write("LOW COVERAGE FILES (<50%):\n")
        for filepath, coverage in analysis['low_coverage_files']:
            f.write(f"  - {filepath}: {coverage:.1f}%\n")
    
    print(f"Detailed report saved to: {report_path}")
    print()


if __name__ == "__main__":
    main()
