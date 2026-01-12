#!/usr/bin/env python3
"""Audit entire project for hardcoded table references.

Scans all .sql files in models/, analyses/, and macros/ directories.
Use this to identify existing issues at scale.
"""

import re
import sys
from pathlib import Path

# Pattern for three-part table references in FROM/JOIN clauses
# Matches: FROM db.schema.table or JOIN db.schema.table
HARDCODED_PATTERN = r'(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)'

# Also check for quoted identifiers: "DB"."SCHEMA"."TABLE"
QUOTED_PATTERN = r'(?:FROM|JOIN)\s+("?[A-Za-z_][A-Za-z0-9_]*"?\s*\.\s*"?[A-Za-z_][A-Za-z0-9_]*"?\s*\.\s*"?[A-Za-z_][A-Za-z0-9_]*"?)'

DIRS_TO_CHECK = ['models', 'analyses', 'macros']


def remove_comments_and_jinja(content: str) -> str:
    """Remove SQL comments and jinja blocks to avoid false positives."""
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    content = re.sub(r'--.*$', '', content, flags=re.MULTILINE)
    # Remove all jinja blocks
    content = re.sub(r'\{\{.*?\}\}', '', content, flags=re.DOTALL)
    content = re.sub(r'\{%.*?%\}', '', content, flags=re.DOTALL)
    return content


def check_file(filepath: Path) -> list[str]:
    """Check a single file for hardcoded references."""
    seen = set()
    issues = []
    content = filepath.read_text(encoding='utf-8', errors='ignore')
    cleaned = remove_comments_and_jinja(content)

    for pattern in [HARDCODED_PATTERN, QUOTED_PATTERN]:
        matches = re.finditer(pattern, cleaned, re.IGNORECASE)
        for match in matches:
            table_ref = match.group(1)
            if 'information_schema' in table_ref.lower():
                continue
            pos = match.start()
            line_num = cleaned[:pos].count('\n') + 1
            key = (line_num, table_ref)
            if key not in seen:
                seen.add(key)
                issues.append(f"  Line {line_num}: {table_ref}")

    return issues


def main() -> int:
    root = Path('.')
    all_issues: dict[str, list[str]] = {}
    files_checked = 0

    for dir_name in DIRS_TO_CHECK:
        dir_path = root / dir_name
        if not dir_path.exists():
            continue

        for sql_file in dir_path.rglob('*.sql'):
            if 'dbt_packages' in str(sql_file):
                continue

            files_checked += 1
            issues = check_file(sql_file)
            if issues:
                all_issues[str(sql_file)] = issues

    print(f"Scanned {files_checked} files in {', '.join(DIRS_TO_CHECK)}\n")

    if all_issues:
        print(f"FOUND {len(all_issues)} files with hardcoded table references:\n")
        for filepath, issues in sorted(all_issues.items()):
            print(f"{filepath}:")
            for issue in issues:
                print(issue)
            print()
        print("Use ref() or source() instead of direct table references.")
        print("See: https://docs.getdbt.com/reference/dbt-jinja-functions/ref")
        print("See: https://docs.getdbt.com/reference/dbt-jinja-functions/source")
        return 1

    print("No hardcoded table references found.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
