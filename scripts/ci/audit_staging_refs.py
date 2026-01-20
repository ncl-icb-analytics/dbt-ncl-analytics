#!/usr/bin/env python3
"""Audit entire project for raw/source references outside staging.

Scans all .sql files in models/ (excluding staging/ and raw/).
Use this to identify existing issues at scale.
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
    root = Path('.')
    models_dir = root / 'models'
    all_issues: dict[str, list[str]] = {}
    files_checked = 0

    if not models_dir.exists():
        print("No models directory found.")
        return 0

    for sql_file in models_dir.rglob('*.sql'):
        if 'dbt_packages' in str(sql_file):
            continue

        path_str = str(sql_file).replace('\\', '/')
        if '/staging/' in path_str or '/raw/' in path_str:
            continue

        files_checked += 1
        issues = check_file(sql_file)
        if issues:
            all_issues[str(sql_file)] = issues

    print(f"Scanned {files_checked} model files (excluding staging/ and raw/)\n")

    if all_issues:
        print(f"FOUND {len(all_issues)} files with raw/source references:\n")
        for filepath, issues in sorted(all_issues.items()):
            print(f"{filepath}:")
            for issue in issues:
                print(f"  - {issue}")
            print()
        print("Only models in staging/ should reference raw models or sources.")
        print("Other models should reference staging models instead.")
        print("See: https://docs.getdbt.com/best-practices/how-we-structure/2-staging")
        return 1

    print("No raw/source references found outside staging.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
