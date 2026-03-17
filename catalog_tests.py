#!/opt/homebrew/Caskroom/miniforge/base/envs/machinefabric/bin/python
"""
Test Catalog Generator for CapDag

Scans all Rust test files and generates a markdown table cataloging all numbered tests
with their descriptions.
"""

import os
import re
from pathlib import Path
from typing import List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class TestInfo:
    """Information about a single test"""
    number: str
    function_name: str
    description: str
    file_path: str
    line_number: int


def extract_test_info(file_path: Path) -> List[TestInfo]:
    """
    Extract test information from a Rust source file.

    Returns a list of TestInfo objects for all numbered tests found.
    """
    tests = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Warning: Could not read {file_path}: {e}")
        return tests

    i = 0
    while i < len(lines):
        line = lines[i]

        # Look for test function definitions: fn test123_something()
        test_match = re.match(r'\s*(?:async\s+)?fn\s+(test(\d+)_\w+)\s*\(', line)

        if test_match:
            function_name = test_match.group(1)
            test_number = test_match.group(2)

            # Look backwards for comment lines and test attribute
            description_lines = []
            j = i - 1

            # Skip the #[test] or #[tokio::test] attribute
            while j >= 0 and lines[j].strip() in ['#[test]', '#[tokio::test]', '']:
                j -= 1

            # Collect comment lines (typically two lines before the test)
            while j >= 0 and lines[j].strip().startswith('//'):
                comment_line = lines[j].strip()
                # Remove the '//' prefix and leading/trailing whitespace
                comment_text = comment_line[2:].strip()
                description_lines.insert(0, comment_text)
                j -= 1

            # Join description lines with space
            description = ' '.join(description_lines)

            # Get relative path from capdag root
            relative_path = file_path.relative_to(file_path.parents[0].parent.parent)

            test_info = TestInfo(
                number=test_number,
                function_name=function_name,
                description=description,
                file_path=str(relative_path),
                line_number=i + 1
            )
            tests.append(test_info)

        i += 1

    return tests


def scan_directory(root_dir: Path) -> List[TestInfo]:
    """
    Recursively scan a directory for Rust test files and extract test information.
    """
    all_tests = []

    for rs_file in root_dir.rglob('*.rs'):
        tests = extract_test_info(rs_file)
        all_tests.extend(tests)

    return all_tests


def generate_markdown_table(tests: List[TestInfo], output_file: str):
    """
    Generate a markdown table cataloging all tests.
    """
    # Sort tests by test number (numerically)
    tests_sorted = sorted(tests, key=lambda t: int(t.number))

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# CapDag Test Catalog\n\n")
        f.write(f"**Total Tests:** {len(tests_sorted)}\n\n")
        f.write("This catalog lists all numbered tests in the capdag codebase.\n\n")

        # Table header
        f.write("| Test # | Function Name | Description | Location |\n")
        f.write("|--------|---------------|-------------|----------|\n")

        # Table rows
        for test in tests_sorted:
            # Escape pipe characters in description
            description = test.description.replace('|', '\\|')

            # Create a shortened function name (remove test### prefix for readability)
            short_name = test.function_name

            # Create file location link
            location = f"{test.file_path}:{test.line_number}"

            f.write(f"| test{test.number} | `{short_name}` | {description} | {location} |\n")

        f.write("\n---\n\n")
        f.write(f"*Generated from capdag source tree*\n")
        f.write(f"*Total numbered tests: {len(tests_sorted)}*\n")


def main():
    """Main entry point"""
    # Determine the capdag root directory (where this script is located)
    script_dir = Path(__file__).parent

    print("Scanning for tests in capdag codebase...")

    # Scan src/ directory
    src_dir = script_dir / 'src'
    if src_dir.exists():
        print(f"  Scanning {src_dir}...")
        src_tests = scan_directory(src_dir)
        print(f"    Found {len(src_tests)} tests in src/")
    else:
        print(f"  Warning: {src_dir} not found")
        src_tests = []

    # Scan testcartridge/ directory
    testcartridge_dir = script_dir / 'testcartridge'
    if testcartridge_dir.exists():
        print(f"  Scanning {testcartridge_dir}...")
        tc_tests = scan_directory(testcartridge_dir)
        print(f"    Found {len(tc_tests)} tests in testcartridge/")
    else:
        print(f"  Warning: {testcartridge_dir} not found")
        tc_tests = []

    # Combine all tests
    all_tests = src_tests + tc_tests

    print(f"\nTotal tests found: {len(all_tests)}")

    # Generate markdown table
    output_file = script_dir / 'TEST_CATALOG.md'
    print(f"\nGenerating catalog: {output_file}")
    generate_markdown_table(all_tests, str(output_file))

    print(f"✓ Catalog generated successfully!")
    print(f"  File: {output_file}")

    # Print some statistics
    test_ranges = {}
    for test in all_tests:
        century = (int(test.number) // 100) * 100
        range_key = f"{century:03d}-{century+99:03d}"
        test_ranges[range_key] = test_ranges.get(range_key, 0) + 1

    print("\nTest distribution by range:")
    for range_key in sorted(test_ranges.keys()):
        count = test_ranges[range_key]
        print(f"  {range_key}: {count} tests")


if __name__ == '__main__':
    main()
