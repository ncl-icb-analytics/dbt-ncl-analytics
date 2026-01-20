#!/usr/bin/env bash
# Check that all models have descriptions.
# Uses yq for YAML parsing. Also checks SQL config blocks.
# Only checks files passed as arguments (typically changed files in a PR).

set -euo pipefail

if [[ $# -eq 0 ]]; then
    echo "PASSED: No files to check."
    exit 0
fi

# Check if yq is available
if ! command -v yq &> /dev/null; then
    echo "Error: yq is required but not installed."
    exit 1
fi

has_description_in_yaml() {
    local model_name="$1"
    local model_dir="$2"

    # Find YAML files in model dir and parent dir
    local yaml_files=()
    while IFS= read -r -d '' f; do
        yaml_files+=("$f")
    done < <(find "$model_dir" -maxdepth 1 -name "*.yml" -o -name "*.yaml" 2>/dev/null | tr '\n' '\0')

    local parent_dir
    parent_dir=$(dirname "$model_dir")
    if [[ -d "$parent_dir" ]]; then
        while IFS= read -r -d '' f; do
            yaml_files+=("$f")
        done < <(find "$parent_dir" -maxdepth 1 -name "*.yml" -o -name "*.yaml" 2>/dev/null | tr '\n' '\0')
    fi

    for yaml_file in "${yaml_files[@]}"; do
        [[ ! -f "$yaml_file" ]] && continue
        # Check if model has non-empty description
        desc=$(yq -r ".models[] | select(.name == \"$model_name\") | .description // \"\"" "$yaml_file" 2>/dev/null || true)
        if [[ -n "$desc" && "$desc" != "null" ]]; then
            return 0
        fi
    done

    return 1
}

has_description_in_sql() {
    local sql_file="$1"
    # Check for description in config block
    if grep -qE '\{\{\s*config\s*\([^)]*description\s*=\s*['"'"'"].+['"'"'"]' "$sql_file" 2>/dev/null; then
        return 0
    fi
    return 1
}

missing=()

for file in "$@"; do
    [[ "$file" != *.sql ]] && continue
    [[ ! -f "$file" ]] && continue
    [[ "$file" == *dbt_packages* ]] && continue
    [[ "$file" != models/* ]] && continue

    model_name=$(basename "$file" .sql)
    model_dir=$(dirname "$file")

    if ! has_description_in_yaml "$model_name" "$model_dir" && ! has_description_in_sql "$file"; then
        missing+=("$file")
    fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
    echo "FAILED: Models missing descriptions:"
    echo ""
    for file in "${missing[@]}"; do
        echo "  - $file"
    done
    echo ""
    echo "Add a description for each model in a corresponding .yml file."
    echo "See: https://docs.getdbt.com/reference/resource-properties/description"
    exit 1
fi

echo "PASSED: All models have descriptions."
exit 0
