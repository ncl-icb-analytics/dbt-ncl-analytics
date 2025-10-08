"""
Selectively remove 'arguments:' property from specific dbt test types in YAML files.

This script targets only tests that fail in Snowflake's older dbt runtime.
Custom tests and some package tests may work fine with arguments:.
"""

import re
from pathlib import Path
import sys


# Test types to process (tests that fail with arguments: property)
# Start with native dbt tests that don't support arguments:
PROCESS_TESTS = [
    'accepted_values',  # Native dbt test
    # Add package tests as we discover they fail
    # 'dbt_utils.accepted_range',
    # 'dbt_utils.expression_is_true',
    # 'dbt_utils.unique_combination_of_columns',
]


def process_yaml_file(filepath, test_types):
    """
    Remove 'arguments:' from specific test types only.

    Args:
        filepath: Path to the YAML file
        test_types: List of test type names to process

    Returns:
        True if file was modified, False otherwise
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    modified = False
    new_lines = []
    i = 0
    tests_indent_level = None
    current_test_type = None

    while i < len(lines):
        line = lines[i]

        # Track if we're in a tests section
        tests_match = re.match(r'^(\s+)tests:\s*$', line)
        if tests_match:
            tests_indent_level = len(tests_match.group(1))
        elif tests_indent_level is not None:
            current_indent_match = re.match(r'^(\s*)\S', line)
            if current_indent_match:
                current_indent = len(current_indent_match.group(1))
                if current_indent <= tests_indent_level:
                    tests_indent_level = None
                    current_test_type = None

        # Check what test type we're in
        if tests_indent_level is not None:
            for test_type in test_types:
                # Match test declaration like "- accepted_values:" or "- dbt_utils.test_name:"
                if re.match(rf'^\s+- {re.escape(test_type)}:\s*$', line):
                    current_test_type = test_type
                    break

        # Check if this line is 'arguments:' under a target test type
        match = re.match(r'^(\s*)arguments:\s*$', line)

        if match and current_test_type:
            modified = True
            indent = match.group(1)
            indent_level = len(indent)

            # Skip the arguments: line
            i += 1

            # Process all child lines (dedent them)
            while i < len(lines):
                next_line = lines[i]

                # Empty lines - keep as is
                if next_line.strip() == '':
                    new_lines.append(next_line)
                    i += 1
                    continue

                # Check indentation of next line
                next_indent_match = re.match(r'^(\s*)', next_line)
                next_indent_level = len(next_indent_match.group(1))

                # If same or less indentation, we've exited the arguments block
                if next_indent_level <= indent_level:
                    break

                # Dedent by removing one indentation unit (2 spaces)
                dedented_line = re.sub(r'^(\s{2})', '', next_line, count=1)
                new_lines.append(dedented_line)
                i += 1
        else:
            new_lines.append(line)
            i += 1

    # Write back if modified
    if modified:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)

    return modified


def main():
    """Process YAML files based on command line arguments."""
    project_root = Path(__file__).parent.parent.parent

    # Parse command line args
    if len(sys.argv) > 1 and sys.argv[1] == '--all':
        # Process all standard tests
        test_types = PROCESS_TESTS
    else:
        # Default: only process tests known to fail
        test_types = ['accepted_values', 'dbt_utils.accepted_range']

    print(f"Processing test types: {', '.join(test_types)}")

    # Find all YAML files
    yaml_files = []
    for pattern in ['models/**/*.yml', 'macros/**/*.yml']:
        yaml_files.extend(project_root.glob(pattern))

    # Filter out dbt_packages
    yaml_files = [f for f in yaml_files if 'dbt_packages' not in str(f)]

    modified_count = 0

    for filepath in yaml_files:
        if process_yaml_file(filepath, test_types):
            modified_count += 1
            print(f"Modified: {filepath.relative_to(project_root)}")

    print(f"\nProcessed {len(yaml_files)} files")
    print(f"Modified {modified_count} files")


if __name__ == '__main__':
    main()
