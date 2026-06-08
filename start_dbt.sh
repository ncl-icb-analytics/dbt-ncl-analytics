#!/usr/bin/env bash
# start_dbt.sh - macOS/Linux equivalent of start_dbt.ps1
#
# Bootstraps the dev environment. Auto-runs when you open a terminal in the
# VS Code workspace (see dbt-ncl-analytics.code-workspace).
#
# dbt runs on the Fusion engine (installed to ~/.local/bin), NOT a Python
# package. The .venv exists only for the Python tooling in scripts/.

# Pin a Fusion version to override "track latest stable" (e.g. 2.0.0-preview.175).
# Leave empty to always track the latest stable release.
FUSION_VERSION_PIN=""

actions=()
install_dir="$HOME/.local/bin"

# Ensure the Fusion install dir is on PATH for this session.
case ":$PATH:" in
    *":$install_dir:"*) ;;
    *) export PATH="$install_dir:$PATH" ;;
esac

# ---------------------------------------------------------------------------
# 1. Git hooks
# ---------------------------------------------------------------------------
echo "Configuring Git hooks..."
if [ "$(git config core.hooksPath)" != ".githooks" ]; then
    git config core.hooksPath .githooks
    echo "[OK] Git hooks configured to use .githooks directory"
else
    echo "[OK] Git hooks already configured"
fi
echo ""

# ---------------------------------------------------------------------------
# 2. Commit signing check
# ---------------------------------------------------------------------------
echo "Checking commit signing..."
gpg_format=$(git config gpg.format)
signing_key=$(git config user.signingkey)
auto_sign=$(git config commit.gpgsign)
if [ "$gpg_format" = "ssh" ] && [ -n "$signing_key" ] && [ "$auto_sign" = "true" ]; then
    echo "[OK] Commit signing configured"
else
    echo "[WARNING] Commit signing is not configured"
    echo "  This repository requires signed commits for branch protection."
    echo "  See CONTRIBUTING.md 'Setting Up Commit Signing' for setup instructions."
    actions+=("Set up commit signing (see CONTRIBUTING.md)")
fi
echo ""

# ---------------------------------------------------------------------------
# 3. dbt Fusion engine
# ---------------------------------------------------------------------------
echo "Checking dbt Fusion engine..."
install_fusion() {
    local version="$1"
    if [ -n "$version" ]; then
        curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update --version "$version"
    else
        curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update
    fi
}

if ! command -v dbt &> /dev/null; then
    echo "[INFO] dbt not found - installing dbt Fusion..."
    if install_fusion "$FUSION_VERSION_PIN"; then
        export PATH="$install_dir:$PATH"
        echo "[OK] dbt Fusion installed"
    else
        echo "[WARNING] Could not install dbt Fusion automatically."
        echo "  Install manually: curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh"
        actions+=("Install dbt Fusion (see CONTRIBUTING.md)")
    fi
elif [ -n "$FUSION_VERSION_PIN" ]; then
    if dbt --version 2>&1 | grep -q "$FUSION_VERSION_PIN"; then
        echo "[OK] dbt Fusion pinned at $FUSION_VERSION_PIN"
    else
        echo "[INFO] Pinning dbt Fusion to $FUSION_VERSION_PIN..."
        install_fusion "$FUSION_VERSION_PIN" && echo "[OK] dbt Fusion pinned at $FUSION_VERSION_PIN"
    fi
else
    # Track latest stable, throttled to one check per day to keep terminals snappy.
    marker="${TMPDIR:-/tmp}/wnl_fusion_update_check"
    today=$(date +%Y-%m-%d)
    last=$([ -f "$marker" ] && cat "$marker" || echo "")
    if [ "$last" != "$today" ]; then
        echo "[INFO] Checking for dbt Fusion updates..."
        dbt system update || echo "[WARNING] Update check failed"
        echo "$today" > "$marker"
    else
        echo "[OK] dbt Fusion checked for updates today"
    fi
fi
command -v dbt &> /dev/null && echo "  $(dbt --version 2>&1 | head -1)"
echo ""

# ---------------------------------------------------------------------------
# 4. Legacy dbt-core cleanup
# ---------------------------------------------------------------------------
# A dbt-core install puts a dbt entrypoint in .venv/bin. Fusion does not
# (it lives in ~/.local/bin), so this marks a pre-Fusion machine.
if [ -f ".venv/bin/dbt" ]; then
    echo "Clearing legacy dbt-core artifacts..."
    for d in target logs; do
        if [ -d "$d" ]; then rm -rf "$d"; echo "  Removed $d/"; fi
    done
    echo "  dbt-core will be pruned from .venv by uv sync below"
    echo ""
fi

# ---------------------------------------------------------------------------
# 5. Python venv for the helper scripts in scripts/ (optional - not needed for dbt)
# ---------------------------------------------------------------------------
echo "Syncing Python tooling for scripts/..."
if command -v uv &> /dev/null; then
    uv sync
    [ -f ".venv/bin/activate" ] && source .venv/bin/activate
    echo "[OK] Python tooling ready"
else
    echo "[INFO] uv not installed - the scripts/ Python tools are unavailable until you install it:"
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
    actions+=("Install uv to run the Python helper scripts (optional)")
fi
echo ""

# Disable AWS metadata service checks (prevents connection pool warnings on Azure)
export AWS_EC2_METADATA_DISABLED=true

# ---------------------------------------------------------------------------
# 6. Load environment variables from .env
# ---------------------------------------------------------------------------
# Fusion auto-loads .env, but we also load it here for the Python scripts and to
# surface the active connection details.
has_snowflake_env=false
if [ -n "$SNOWFLAKE_ACCOUNT" ] && [ -n "$SNOWFLAKE_USER" ] && { [ -n "$SNOWFLAKE_PAT" ] || [ -n "$SNOWFLAKE_PASSWORD" ] || [ "$SNOWFLAKE_AUTHENTICATOR" = "externalbrowser" ]; }; then
    has_snowflake_env=true
fi

echo "Loading environment variables from .env..."
if [ -f ".env" ]; then
    env_count=0
    while IFS= read -r line || [ -n "$line" ]; do
        [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
        if [[ "$line" == *"="* ]]; then
            key="${line%%=*}"
            value="${line#*=}"
            export "$key=$value"
            ((env_count++))
        fi
    done < ".env"
    echo "[OK] Loaded $env_count environment variables"

    if grep -v '^#' ".env" | grep -q '^[^=]*=.*your-.*-here'; then
        echo "[WARNING] .env still contains placeholder values"
        actions+=("Update credentials in .env, then open a new terminal (Ctrl+\`)")
    else
        [ -n "$SNOWFLAKE_ACCOUNT" ] && echo "  SNOWFLAKE_ACCOUNT: ${SNOWFLAKE_ACCOUNT:0:10}..."
        [ -n "$SNOWFLAKE_USER" ] && echo "  SNOWFLAKE_USER: $SNOWFLAKE_USER"
        [ -n "$SNOWFLAKE_ROLE" ] && echo "  SNOWFLAKE_ROLE: $SNOWFLAKE_ROLE"
        [ -n "$SNOWFLAKE_WAREHOUSE" ] && echo "  SNOWFLAKE_WAREHOUSE: $SNOWFLAKE_WAREHOUSE"
        [ -n "$SNOWFLAKE_PAT" ] && echo "  SNOWFLAKE_PAT: ${SNOWFLAKE_PAT:0:8}..."
        [ -n "$SNOWFLAKE_PASSWORD" ] && echo "  SNOWFLAKE_PASSWORD: [set]"
        [ -n "$SNOWFLAKE_AUTHENTICATOR" ] && echo "  SNOWFLAKE_AUTHENTICATOR: $SNOWFLAKE_AUTHENTICATOR"
    fi
else
    if [ "$has_snowflake_env" = true ]; then
        echo "[OK] No .env file found - using existing environment variables"
    elif [ -f "env.example" ]; then
        cp env.example .env
        echo "[WARNING] No .env file found - created from template"
        actions+=("Update credentials in .env, then open a new terminal (Ctrl+\`)")
    else
        echo "[WARNING] No .env file found and no env.example template"
        actions+=("Update credentials in .env, then open a new terminal (Ctrl+\`)")
    fi
fi
echo ""

# ---------------------------------------------------------------------------
# 7. dbt packages
# ---------------------------------------------------------------------------
if [ ! -d "dbt_packages" ]; then
    echo "Installing dbt packages (dbt deps)..."
    dbt deps || actions+=("Run 'dbt deps' to install dbt packages")
    echo ""
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
if [ ${#actions[@]} -gt 0 ]; then
    echo "To finish setup:"
    for action in "${actions[@]}"; do
        echo "  -> $action"
    done
else
    echo "Ready! dbt runs on the Fusion engine."
    echo "Try: dbt debug"
fi
