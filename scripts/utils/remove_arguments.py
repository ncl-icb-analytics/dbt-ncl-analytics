"""
Remove 'arguments:' property from dbt test definitions in YAML files.

This script converts dbt v1.10+ test syntax back to v1.8/v1.9 compatible syntax
by unwrapping the arguments property and dedenting its children.
"""

import re
from pathlib import Path


def process_yaml_file(filepath):
    """
    Remove 'arguments:' lines and dedent their children in a YAML file.
    Only processes arguments within tests sections, not macro arguments.

    Args:
        filepath: Path to the YAML file to process

    Returns:
        True if file was modified, False otherwise
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    modified = False
    new_lines = []
    i = 0
    tests_indent_level = None

    while i < len(lines):
        line = lines[i]

        # Track if we're in a tests section by checking indentation
        tests_match = re.match(r'^(\s+)tests:\s*$', line)
        if tests_match:
            tests_indent_level = len(tests_match.group(1))
        elif tests_indent_level is not None:
            # Check if we've exited tests section (line at same or less indentation)
            current_indent_match = re.match(r'^(\s*)\S', line)
            if current_indent_match:
                current_indent = len(current_indent_match.group(1))
                if current_indent <= tests_indent_level:
                    tests_indent_level = None

        # Check if this line is 'arguments:' (with any indentation)
        match = re.match(r'^(\s*)arguments:\s*$', line)

        # Only process if we're in a tests section
        if match and tests_indent_level is not None:
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

                # Dedent by removing the first indentation unit
                # Typically this is 2 spaces in YAML
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
    """Process all YAML files in models and macros directories."""
    project_root = Path(__file__).parent.parent.parent

    # Find all YAML files in models and macros, excluding dbt_packages
    yaml_files = []
    for pattern in ['models/**/*.yml', 'macros/**/*.yml']:
        yaml_files.extend(project_root.glob(pattern))

    # Filter out dbt_packages
    yaml_files = [f for f in yaml_files if 'dbt_packages' not in str(f)]

    modified_count = 0

    for filepath in yaml_files:
        if process_yaml_file(filepath):
            modified_count += 1
            print(f"Modified: {filepath.relative_to(project_root)}")

    print(f"\nProcessed {len(yaml_files)} files")
    print(f"Modified {modified_count} files")


if __name__ == '__main__':
    main()
