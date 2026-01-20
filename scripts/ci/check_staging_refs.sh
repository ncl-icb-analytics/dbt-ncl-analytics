#!/usr/bin/env bash
# Check that only staging models reference raw models or sources.
# Only checks files passed as arguments (typically changed files in a PR).
# Models outside of models/staging/ and models/raw/ should not directly
# reference raw_* models or use source().

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

    # Skip staging and raw directories (they're allowed)
    [[ "$file" == */staging/* ]] && continue
    [[ "$file" == */raw/* ]] && continue

    # Only check files in models/
    [[ "$file" != models/* ]] && continue

    issues=""

    # Check for raw_* model references
    if grep -qE '\{\{\s*ref\s*\(\s*['"'"'"]raw_' "$file"; then
        issues+="  - References raw_* model directly"$'\n'
    fi

    # Check for source() usage
    if grep -qE '\{\{\s*source\s*\(' "$file"; then
        issues+="  - Uses source() directly"$'\n'
    fi

    if [[ -n "$issues" ]]; then
        file_issues["$file"]="$issues"
        failed=1
    fi
done

if [[ $failed -eq 1 ]]; then
    echo "FAILED: Found raw/source references outside staging:"
    echo ""
    for file in "${!file_issues[@]}"; do
        echo "$file:"
        echo -n "${file_issues[$file]}"
        echo ""
    done
    echo "Only models in staging/ should reference raw models or sources."
    echo "Other models should reference staging models instead."
    echo "See: https://docs.getdbt.com/best-practices/how-we-structure/2-staging"
    exit 1
fi

echo "PASSED: All raw/source references are in staging models."
exit 0
