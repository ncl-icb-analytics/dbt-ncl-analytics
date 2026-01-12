#!/usr/bin/env python3
"""Check for hardcoded table references in dbt SQL files.

Only checks files passed as arguments (typically changed files in a PR).
"""

import re
import sys
from pathlib import Path

# Pattern for three-part table references in FROM/JOIN clauses
HARDCODED_PATTERN = r'(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)'

# Also check for quoted identifiers: "DB"."SCHEMA"."TABLE"
QUOTED_PATTERN = r'(?:FROM|JOIN)\s+("?[A-Za-z_][A-Za-z0-9_]*"?\s*\.\s*"?[A-Za-z_][A-Za-z0-9_]*"?\s*\.\s*"?[A-Za-z_][A-Za-z0-9_]*"?)'


def remove_comments_and_jinja(content: str) -> str:
    """Remove SQL comments and jinja blocks to avoid false positives."""
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    content = re.sub(r'--.*$', '', content, flags=re.MULTILINE)
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

        issues = check_file(filepath)
        if issues:
            all_issues[str(filepath)] = issues

    if all_issues:
        print("FAILED: Found hardcoded table references:\n")
        for filepath, issues in sorted(all_issues.items()):
            print(f"{filepath}:")
            for issue in issues:
                print(issue)
            print()
        print("Use ref() or source() instead of direct table references.")
        print("See: https://docs.getdbt.com/reference/dbt-jinja-functions/ref")
        print("See: https://docs.getdbt.com/reference/dbt-jinja-functions/source")
        return 1

    print("PASSED: No hardcoded table references found.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
