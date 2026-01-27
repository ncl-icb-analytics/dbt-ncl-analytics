# start_dbt.ps1

$actions = @()

# Configure Git hooks for additional safety
Write-Host "Configuring Git hooks..." -ForegroundColor Cyan
$currentHooksPath = git config core.hooksPath

if ($currentHooksPath -ne ".githooks") {
    git config core.hooksPath .githooks
    Write-Host "[OK] Git hooks configured to use .githooks directory" -ForegroundColor Green
} else {
    Write-Host "[OK] Git hooks already configured" -ForegroundColor Green
}
Write-Host ""

# Check commit signing
Write-Host "Checking commit signing..." -ForegroundColor Cyan
$gpgFormat = git config gpg.format
$signingKey = git config user.signingkey
$autoSign = git config commit.gpgsign
if ($gpgFormat -eq "ssh" -and $signingKey -and $autoSign -eq "true") {
    Write-Host "[OK] Commit signing configured" -ForegroundColor Green
} else {
    Write-Host "[WARNING] Commit signing is not configured" -ForegroundColor Yellow
    Write-Host "  This repository requires signed commits for branch protection." -ForegroundColor Gray
    Write-Host "  You can run dbt commands, but commits will be rejected without signing." -ForegroundColor Gray
    Write-Host "  See CONTRIBUTING.md 'Setting Up Commit Signing' for setup instructions." -ForegroundColor Gray
    $actions += "Set up commit signing (see CONTRIBUTING.md)"
}
Write-Host ""

# Activate or create virtual environment
Write-Host "Activating Python virtual environment..." -ForegroundColor Cyan
if (Test-Path ".venv\Scripts\Activate.ps1") {
    $venvPath = ".venv\Scripts\Activate.ps1"
} elseif (Test-Path "venv\Scripts\Activate.ps1") {
    $venvPath = "venv\Scripts\Activate.ps1"
} else {
    Write-Host "[INFO] No virtual environment found" -ForegroundColor Cyan
    if (Get-Command uv -ErrorAction SilentlyContinue) {
        Write-Host "[INFO] Running uv sync..." -ForegroundColor Cyan
        uv sync
        if (Test-Path ".venv\Scripts\Activate.ps1") {
            $venvPath = ".venv\Scripts\Activate.ps1"
        } else {
            Write-Host "[ERROR] uv sync did not create a virtual environment" -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "[ERROR] uv is not installed. Install it with:" -ForegroundColor Red
        Write-Host '  powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"' -ForegroundColor Yellow
        Write-Host ""
        Write-Host "  After installing, close and reopen VS Code so uv is on your PATH." -ForegroundColor Gray
        exit 1
    }
}
& $venvPath

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Virtual environment activated" -ForegroundColor Green
    Write-Host "  Python: $(python --version 2>&1)" -ForegroundColor Gray
} else {
    Write-Host "[ERROR] Failed to activate virtual environment" -ForegroundColor Red
    Write-Host "  Run 'uv sync' or 'python -m venv venv' to create one" -ForegroundColor Yellow
    exit 1
}

# Keep dependencies in sync with lockfile
if (Get-Command uv -ErrorAction SilentlyContinue) {
    uv sync
    Write-Host "[OK] Dependencies up to date" -ForegroundColor Green
}
Write-Host ""

# Disable AWS metadata service checks (prevents connection pool warnings on Azure)
[System.Environment]::SetEnvironmentVariable('AWS_EC2_METADATA_DISABLED', 'true', 'Process')

# Load project-specific environment variables
Write-Host "Loading environment variables from .env..." -ForegroundColor Cyan

$envPath = ".env"
if (Test-Path $envPath) {
    $envCount = 0
    Get-Content $envPath | ForEach-Object {
      if ($_ -match '^([^=]+)=(.*)$' -and -not $_.StartsWith('#')) {
          [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
          $envCount++
      }
    }
    Write-Host "[OK] Loaded $envCount environment variables" -ForegroundColor Green

    # Detect placeholder credentials — check uncommented KEY=value lines for template values
    $hasPlaceholders = (Get-Content $envPath | Where-Object { $_ -match '^[^#].*=.*your-.*-here' } | Measure-Object).Count -gt 0
    if ($hasPlaceholders) {
        Write-Host "[WARNING] .env still contains placeholder values" -ForegroundColor Yellow
        $actions += 'Update credentials in .env, then open a new terminal (Ctrl+`)'
    } else {
        # Show key variables (without exposing sensitive values)
        if ($env:SNOWFLAKE_ACCOUNT) {
            Write-Host "  SNOWFLAKE_ACCOUNT: $($env:SNOWFLAKE_ACCOUNT.Substring(0, [Math]::Min(10, $env:SNOWFLAKE_ACCOUNT.Length)))..." -ForegroundColor Gray
        }
        if ($env:SNOWFLAKE_USER) {
            Write-Host "  SNOWFLAKE_USER: $env:SNOWFLAKE_USER" -ForegroundColor Gray
        }
        if ($env:SNOWFLAKE_ROLE) {
            Write-Host "  SNOWFLAKE_ROLE: $env:SNOWFLAKE_ROLE" -ForegroundColor Gray
        }
        if ($env:SNOWFLAKE_WAREHOUSE) {
            Write-Host "  SNOWFLAKE_WAREHOUSE: $env:SNOWFLAKE_WAREHOUSE" -ForegroundColor Gray
        }
    }
} else {
    if (Test-Path "env.example") {
        Copy-Item "env.example" ".env"
        Write-Host "[WARNING] No .env file found — created from template" -ForegroundColor Yellow
    } else {
        Write-Host "[WARNING] No .env file found and no env.example template" -ForegroundColor Yellow
    }
    $actions += 'Update credentials in .env, then open a new terminal (Ctrl+`)'
}
Write-Host ""

# Check for dbt packages
if (-not (Test-Path "dbt_packages")) {
    Write-Host "[WARNING] No dbt_packages directory found" -ForegroundColor Yellow
    $actions += "Run 'dbt deps' to install dbt packages"
    Write-Host ""
}

# Summary
if ($actions.Count -gt 0) {
    Write-Host "To finish setup:" -ForegroundColor Yellow
    foreach ($action in $actions) {
        Write-Host "  -> $action" -ForegroundColor Gray
    }
} else {
    Write-Host "Ready! You can now run dbt commands." -ForegroundColor Green
    Write-Host "Try: dbt debug (to test your connection)" -ForegroundColor Gray
}
