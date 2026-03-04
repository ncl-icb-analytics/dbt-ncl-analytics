#!/usr/bin/env bash
# start_dbt.sh - macOS/Linux equivalent of start_dbt.ps1

actions=()

# Configure Git hooks
echo "Configuring Git hooks..."
current_hooks_path=$(git config core.hooksPath)
if [ "$current_hooks_path" != ".githooks" ]; then
    git config core.hooksPath .githooks
    echo "[OK] Git hooks configured to use .githooks directory"
else
    echo "[OK] Git hooks already configured"
fi
echo ""

# Check commit signing
echo "Checking commit signing..."
gpg_format=$(git config gpg.format)
signing_key=$(git config user.signingkey)
auto_sign=$(git config commit.gpgsign)
if [ "$gpg_format" = "ssh" ] && [ -n "$signing_key" ] && [ "$auto_sign" = "true" ]; then
    echo "[OK] Commit signing configured"
else
    echo "[WARNING] Commit signing is not configured"
    echo "  This repository requires signed commits for branch protection."
    echo "  You can run dbt commands, but commits will be rejected without signing."
    echo "  See CONTRIBUTING.md 'Setting Up Commit Signing' for setup instructions."
    actions+=("Set up commit signing (see CONTRIBUTING.md)")
fi
echo ""

# Activate or create virtual environment
echo "Activating Python virtual environment..."
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "[INFO] No virtual environment found"
    if command -v uv &> /dev/null; then
        echo "[INFO] Running uv sync..."
        uv sync
        if [ -f ".venv/bin/activate" ]; then
            source .venv/bin/activate
        else
            echo "[ERROR] uv sync did not create a virtual environment"
            return 1 2>/dev/null || exit 1
        fi
    else
        echo "[ERROR] uv is not installed. Install it with one of:"
        echo "  brew install uv"
        echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
        echo ""
        echo "  After installing, close and reopen VS Code so uv is on your PATH."
        return 1 2>/dev/null || exit 1
    fi
fi
echo "[OK] Virtual environment activated"
echo "  Python: $(python --version 2>&1)"

# Keep dependencies in sync with lockfile
if command -v uv &> /dev/null; then
    uv sync
    echo "[OK] Dependencies up to date"
fi
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
        # Extract key=value (compatible with both bash and zsh)
        if [[ "$line" == *"="* ]]; then
            key="${line%%=*}"
            value="${line#*=}"
            export "$key=$value"
            ((env_count++))
        fi
    done < ".env"
    echo "[OK] Loaded $env_count environment variables"

    # Detect placeholder credentials — check uncommented KEY=value lines for template values
    if grep -v '^#' ".env" | grep -q '^[^=]*=.*your-.*-here'; then
        echo "[WARNING] .env still contains placeholder values"
        actions+=("Update credentials in .env, then open a new terminal (Ctrl+\`)")
    else
        # Show key variables (without exposing sensitive values)
        [ -n "$SNOWFLAKE_ACCOUNT" ] && echo "  SNOWFLAKE_ACCOUNT: ${SNOWFLAKE_ACCOUNT:0:10}..."
        [ -n "$SNOWFLAKE_USER" ] && echo "  SNOWFLAKE_USER: $SNOWFLAKE_USER"
        [ -n "$SNOWFLAKE_ROLE" ] && echo "  SNOWFLAKE_ROLE: $SNOWFLAKE_ROLE"
        [ -n "$SNOWFLAKE_WAREHOUSE" ] && echo "  SNOWFLAKE_WAREHOUSE: $SNOWFLAKE_WAREHOUSE"
    fi
else
    if [ -f "env.example" ]; then
        cp env.example .env
        echo "[WARNING] No .env file found — created from template"
    else
        echo "[WARNING] No .env file found and no env.example template"
    fi
    actions+=("Update credentials in .env, then open a new terminal (Ctrl+\`)")
fi
echo ""

# Check for dbt packages
if [ ! -d "dbt_packages" ]; then
    echo "[WARNING] No dbt_packages directory found"
    actions+=("Run 'dbt deps' to install dbt packages")
    echo ""
fi

# Summary
if [ ${#actions[@]} -gt 0 ]; then
    echo "To finish setup:"
    for action in "${actions[@]}"; do
        echo "  -> $action"
    done
else
    echo "Ready! You can now run dbt commands."
    echo "Try: dbt debug (to test your connection)"
fi
