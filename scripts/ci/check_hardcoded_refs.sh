#!/usr/bin/env bash
# Check for hardcoded table references in dbt SQL files.
# Only checks files passed as arguments (typically changed files in a PR).

set -euo pipefail

if [[ $# -eq 0 ]]; then
    echo "PASSED: No files to check."
    exit 0
fi

failed=0
declare -A file_issues

for file in "$@"; do
    [[ "$file" != *.sql ]] && continue
    [[ ! -f "$file" ]] && continue
    [[ "$file" == *dbt_packages* ]] && continue

    # Remove comments and jinja, then check for hardcoded refs
    # Pattern: FROM/JOIN followed by three-part identifier (DB.SCHEMA.TABLE)
    issues=$(sed -e 's/--.*$//' -e 's|/\*.*\*/||g' "$file" | \
        tr -d '\n' | \
        sed -e 's/{{[^}]*}}//g' -e 's/{%[^%]*%}//g' | \
        grep -oiE '(FROM|JOIN)\s+[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*' | \
        grep -viE 'information_schema' | \
        sed 's/^/  /' || true)

    if [[ -n "$issues" ]]; then
        file_issues["$file"]="$issues"
        failed=1
    fi
done

if [[ $failed -eq 1 ]]; then
    echo "FAILED: Found hardcoded table references:"
    echo ""
    for file in "${!file_issues[@]}"; do
        echo "$file:"
        echo "${file_issues[$file]}"
        echo ""
    done
    echo "Use ref() or source() instead of direct table references."
    echo "See: https://docs.getdbt.com/reference/dbt-jinja-functions/ref"
    echo "See: https://docs.getdbt.com/reference/dbt-jinja-functions/source"
    exit 1
fi

echo "PASSED: No hardcoded table references found."
exit 0
