#!/usr/bin/env bash
# start_dbt.sh - macOS/Linux equivalent of start_dbt.ps1

# Configure Git hooks
echo "Configuring Git hooks..."
current_hooks_path=$(git config core.hooksPath)
if [ "$current_hooks_path" != ".githooks" ]; then
    git config core.hooksPath .githooks
    echo "[OK] Git hooks configured to use .githooks directory"
    echo "  Pre-commit hook will warn before committing profiles.yml"
else
    echo "[OK] Git hooks already configured"
fi
echo ""

# Check if profiles.yml exists
echo "Checking profiles.yml configuration..."
if [ ! -f "profiles.yml" ]; then
    echo "[WARNING] No profiles.yml found"
    echo "  Copy profiles.yml.template to profiles.yml and configure your credentials"
    echo "  For Snowflake native execution, profiles.yml is not required"
else
    echo "[OK] profiles.yml found"
fi
echo ""

# Activate virtual environment
echo "Activating Python virtual environment..."
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "[ERROR] No virtual environment found"
    echo "  Run 'uv sync' or 'python -m venv venv' to create one"
    return 1 2>/dev/null || exit 1
fi
echo "[OK] Virtual environment activated"
echo "  Python: $(python --version 2>&1)"
echo ""

# Disable AWS metadata service checks (prevents connection pool warnings on Azure)
export AWS_EC2_METADATA_DISABLED=true

# Load project-specific environment variables
echo "Loading environment variables from .env..."
if [ -f ".env" ]; then
    env_count=0
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip comments and empty lines
        [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
        if [[ "$line" =~ ^([^=]+)=(.*)$ ]]; then
            export "${BASH_REMATCH[1]}=${BASH_REMATCH[2]}"
            ((env_count++))
        fi
    done < ".env"
    echo "[OK] Loaded $env_count environment variables"

    # Show key variables (without exposing sensitive values)
    [ -n "$SNOWFLAKE_ACCOUNT" ] && echo "  SNOWFLAKE_ACCOUNT: ${SNOWFLAKE_ACCOUNT:0:10}..."
    [ -n "$SNOWFLAKE_USER" ] && echo "  SNOWFLAKE_USER: $SNOWFLAKE_USER"
    [ -n "$SNOWFLAKE_ROLE" ] && echo "  SNOWFLAKE_ROLE: $SNOWFLAKE_ROLE"
    [ -n "$SNOWFLAKE_WAREHOUSE" ] && echo "  SNOWFLAKE_WAREHOUSE: $SNOWFLAKE_WAREHOUSE"
else
    echo "[WARNING] No .env file found"
    echo "  Copy env.example to .env and add your credentials"
fi
echo ""

echo "Ready! You can now run dbt commands."
echo "Try: dbt debug (to test your connection)"
