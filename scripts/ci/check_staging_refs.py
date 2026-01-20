#!/usr/bin/env python3
"""Check that only staging models reference raw models or sources.

Only checks files passed as arguments (typically changed files in a PR).
Models outside of models/staging/ and models/raw/ should not directly
reference raw_* models or use source().
"""

import re
import sys
from pathlib import Path

RAW_REF_PATTERN = r"{{\s*ref\s*\(\s*['\"]raw_"
SOURCE_PATTERN = r"{{\s*source\s*\("


def check_file(filepath: Path) -> list[str]:
    """Check a single file for raw/source references."""
    issues = []
    content = filepath.read_text(encoding='utf-8', errors='ignore')

    if re.search(RAW_REF_PATTERN, content):
        issues.append("References raw_* model directly")

    if re.search(SOURCE_PATTERN, content):
        issues.append("Uses source() directly")

    return issues


def main() -> int:
    if len(sys.argv) < 2:
        print("PASSED: No files to check.")
        return 0

    files = [Path(f) for f in sys.argv[1:] if f.endswith('.sql')]
    all_issues: dict[str, list[str]] = {}

    for filepath in files:
        if not filepath.exists():
            continue
        if 'dbt_packages' in str(filepath):
            continue

        # Skip models in staging/ and raw/ (they're allowed)
        path_str = str(filepath).replace('\\', '/')
        if '/staging/' in path_str or '/raw/' in path_str:
            continue

        # Only check files in models/
        if not path_str.startswith('models/'):
            continue

        issues = check_file(filepath)
        if issues:
            all_issues[str(filepath)] = issues

    if all_issues:
        print("FAILED: Found raw/source references outside staging:\n")
        for filepath, issues in sorted(all_issues.items()):
            print(f"{filepath}:")
            for issue in issues:
                print(f"  - {issue}")
            print()
        print("Only models in staging/ should reference raw models or sources.")
        print("Other models should reference staging models instead.")
        print("See: https://docs.getdbt.com/best-practices/how-we-structure/2-staging")
        return 1

    print("PASSED: All raw/source references are in staging models.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
