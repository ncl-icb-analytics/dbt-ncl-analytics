#!/usr/bin/env bash
# Check that all models have at least one test defined.
# Uses yq for YAML parsing.
# Only checks files passed as arguments (typically changed files in a PR).
# Tests can be defined at the model level or column level.

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

model_has_tests() {
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

        # Check for model-level tests or data_tests
        model_tests=$(yq -r ".models[] | select(.name == \"$model_name\") | (.tests // .data_tests) | length" "$yaml_file" 2>/dev/null || echo "0")
        if [[ "$model_tests" != "0" && "$model_tests" != "null" && -n "$model_tests" ]]; then
            return 0
        fi

        # Check for column-level tests
        col_tests=$(yq -r ".models[] | select(.name == \"$model_name\") | .columns[]? | (.tests // .data_tests) | length" "$yaml_file" 2>/dev/null || echo "")
        for count in $col_tests; do
            if [[ "$count" != "0" && "$count" != "null" && -n "$count" ]]; then
                return 0
            fi
        done
    done

    return 1
}

missing=()

for file in "$@"; do
    [[ "$file" != *.sql ]] && continue
    [[ ! -f "$file" ]] && continue
    [[ "$file" == *dbt_packages* ]] && continue
    [[ "$file" != models/* ]] && continue

    # Skip raw/ layer - tests not required
    [[ "$file" == */raw/* ]] && continue

    model_name=$(basename "$file" .sql)
    model_dir=$(dirname "$file")

    if ! model_has_tests "$model_name" "$model_dir"; then
        missing+=("$file")
    fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
    echo "FAILED: Models missing tests:"
    echo ""
    for file in "${missing[@]}"; do
        echo "  - $file"
    done
    echo ""
    echo "Add at least one test for each model in a corresponding .yml file."
    echo "See: https://docs.getdbt.com/docs/build/data-tests"
    exit 1
fi

echo "PASSED: All models have tests."
exit 0
