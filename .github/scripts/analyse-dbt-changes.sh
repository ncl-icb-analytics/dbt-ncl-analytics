#!/bin/bash
# Analyses dbt model changes between commits and generates impact summary

set -e

# Get the last release tag, or fall back to first commit if no tags exist
LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || git rev-list --max-parents=0 HEAD)
CURRENT_REF=${1:-HEAD}

echo "Analysing changes from $LAST_TAG to $CURRENT_REF"
echo ""

# Function to determine layer from file path
get_layer() {
    local file=$1
    if [[ $file == models/raw/* ]]; then echo "Raw"
    elif [[ $file == models/staging/* ]]; then echo "Staging"
    elif [[ $file == models/modelling/* ]]; then echo "Modelling"
    elif [[ $file == models/reporting/* ]]; then echo "Reporting"
    elif [[ $file == models/published/* ]]; then echo "Published"
    else echo "Other"
    fi
}

# Function to determine database from file path
get_database() {
    local file=$1
    if [[ $file == models/raw/* ]]; then echo "MODELLING"
    elif [[ $file == models/staging/* ]]; then echo "MODELLING"
    elif [[ $file == models/modelling/* ]]; then echo "MODELLING"
    elif [[ $file == models/reporting/* ]]; then echo "REPORTING"
    elif [[ $file == models/published/direct_care/* ]]; then echo "PUBLISHED_REPORTING__DIRECT_CARE"
    elif [[ $file == models/published/secondary_use/* ]]; then echo "PUBLISHED_REPORTING__SECONDARY_USE"
    else echo "UNKNOWN"
    fi
}

# Function to extract schema from file path
get_schema() {
    local file=$1
    local layer=$(get_layer "$file")

    # Extract subdomain for auto-schema domains (olids)
    if [[ $file == models/*/olids/* ]]; then
        # Extract the subdomain folder after olids/
        local subdomain=$(echo "$file" | sed -n 's|models/.*/olids/\([^/]*\)/.*|\1|p')
        if [[ -n "$subdomain" ]]; then
            echo "OLIDS_${subdomain^^}"
        else
            echo "OLIDS"
        fi
    elif [[ $file == models/staging/* ]]; then
        echo "DBT_STAGING"
    elif [[ $file == models/modelling/commissioning/* ]]; then
        echo "COMMISSIONING_MODELLING"
    elif [[ $file == models/reporting/commissioning/* ]]; then
        echo "COMMISSIONING_REPORTING"
    elif [[ $file == models/published/secondary_use/commissioning/* ]]; then
        echo "COMMISSIONING_PUBLISHED"
    elif [[ $file == models/*/shared/* ]]; then
        echo "REFERENCE"
    else
        echo "VARIOUS"
    fi
}

# Temporary files for tracking
declare -A added_by_layer
declare -A modified_by_layer
declare -A deleted_by_layer
declare -A schemas_affected

# Get all SQL file changes
while IFS= read -r line; do
    status="${line:0:1}"
    file="${line:2}"

    # Only process .sql files in models/
    if [[ $file == models/*.sql ]]; then
        layer=$(get_layer "$file")
        schema=$(get_schema "$file")

        # Track schema
        schemas_affected["$schema"]=1

        # Track by status
        case $status in
            A)
                added_by_layer["$layer"]=$((${added_by_layer[$layer]:-0} + 1))
                echo "✨ Added: $file → $schema"
                ;;
            M)
                modified_by_layer["$layer"]=$((${modified_by_layer[$layer]:-0} + 1))
                echo "📝 Modified: $file → $schema"
                ;;
            D)
                deleted_by_layer["$layer"]=$((${deleted_by_layer[$layer]:-0} + 1))
                echo "🗑️  Deleted: $file → $schema"
                ;;
        esac
    fi
done < <(git diff --name-status "$LAST_TAG" "$CURRENT_REF")

echo ""
echo "## Impact Summary"
echo ""
echo "| Layer | Database | Added | Modified | Deleted |"
echo "|-------|----------|-------|----------|---------|"

# Output summary for each layer
for layer in "Raw" "Staging" "Modelling" "Reporting" "Published"; do
    added=${added_by_layer[$layer]:-0}
    modified=${modified_by_layer[$layer]:-0}
    deleted=${deleted_by_layer[$layer]:-0}

    # Only show layers with changes
    if [ $added -gt 0 ] || [ $modified -gt 0 ] || [ $deleted -gt 0 ]; then
        # Determine typical database for layer
        case $layer in
            Raw|Staging|Modelling) db="MODELLING" ;;
            Reporting) db="REPORTING" ;;
            Published) db="PUBLISHED_REPORTING" ;;
        esac

        echo "| $layer | $db | $added | $modified | $deleted |"
    fi
done

echo ""
echo "**Schemas affected**: ${!schemas_affected[@]}"
echo ""
