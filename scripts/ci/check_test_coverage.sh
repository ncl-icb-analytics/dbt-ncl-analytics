#!/usr/bin/env bash
# Check test coverage for dbt models.
# Models in models/ (excluding raw/) must have at least one test.
# Uses yq for YAML parsing.

set -euo pipefail

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
    done < <(find "$model_dir" -maxdepth 1 \( -name "*.yml" -o -name "*.yaml" \) -print0 2>/dev/null)

    local parent_dir
    parent_dir=$(dirname "$model_dir")
    if [[ -d "$parent_dir" ]]; then
        while IFS= read -r -d '' f; do
            yaml_files+=("$f")
        done < <(find "$parent_dir" -maxdepth 1 \( -name "*.yml" -o -name "*.yaml" \) -print0 2>/dev/null)
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

total=0
with_tests=0
missing=()

# Find all SQL files in models/ excluding raw/ and dbt_packages
while IFS= read -r -d '' file; do
    [[ "$file" == *dbt_packages* ]] && continue
    [[ "$file" == */raw/* ]] && continue

    ((total++)) || true

    model_name=$(basename "$file" .sql)
    model_dir=$(dirname "$file")

    if model_has_tests "$model_name" "$model_dir"; then
        ((with_tests++)) || true
    else
        missing+=("$file")
    fi
done < <(find models -name "*.sql" -print0 2>/dev/null)

if [[ $total -eq 0 ]]; then
    pct=0
else
    pct=$((with_tests * 100 / total))
fi

echo "Models with tests: $with_tests/$total ($pct%)"

if [[ ${#missing[@]} -gt 0 ]]; then
    echo ""
    echo "Models missing tests (${#missing[@]}):"
    count=0
    for file in "${missing[@]}"; do
        echo "  - $file"
        ((count++)) || true
        if [[ $count -ge 20 ]]; then
            remaining=$((${#missing[@]} - 20))
            if [[ $remaining -gt 0 ]]; then
                echo "  ... and $remaining more"
            fi
            break
        fi
    done
fi

# Determine badge color
if [[ $pct -ge 80 ]]; then
    color="brightgreen"
elif [[ $pct -ge 60 ]]; then
    color="green"
elif [[ $pct -ge 40 ]]; then
    color="yellow"
else
    color="red"
fi

echo ""
echo "Badge: $pct% ($color)"

# Output for GitHub Actions
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "total=$total" >> "$GITHUB_OUTPUT"
    echo "covered=$with_tests" >> "$GITHUB_OUTPUT"
    echo "percentage=$pct" >> "$GITHUB_OUTPUT"
fi
