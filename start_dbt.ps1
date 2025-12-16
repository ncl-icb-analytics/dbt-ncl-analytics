# start_dbt.ps1

# Configure Git hooks for additional safety
Write-Host "Configuring Git hooks..." -ForegroundColor Cyan
$currentHooksPath = git config core.hooksPath

if ($currentHooksPath -ne ".githooks") {
    git config core.hooksPath .githooks
    Write-Host "[OK] Git hooks configured to use .githooks directory" -ForegroundColor Green
    Write-Host "  Pre-commit hook will warn before committing profiles.yml" -ForegroundColor Gray
} else {
    Write-Host "[OK] Git hooks already configured" -ForegroundColor Green
}
Write-Host ""

# Check if profiles.yml exists for local development
Write-Host "Checking profiles.yml configuration..." -ForegroundColor Cyan
if (-not (Test-Path "profiles.yml")) {
    Write-Host "[WARNING] No profiles.yml found" -ForegroundColor Yellow
    Write-Host "  Copy profiles.yml.example to profiles.yml and configure your credentials" -ForegroundColor Gray
    Write-Host "  For Snowflake native execution, profiles.yml is not required" -ForegroundColor Gray
    Write-Host ""
} else {
    Write-Host "[OK] profiles.yml found" -ForegroundColor Green
    Write-Host ""
}

# Activate virtual environment
Write-Host "Activating Python virtual environment..." -ForegroundColor Cyan
$venvPath = "venv\Scripts\Activate.ps1"
& $venvPath

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Virtual environment activated" -ForegroundColor Green
    Write-Host "  Python: $(python --version 2>&1)" -ForegroundColor Gray
} else {
    Write-Host "[ERROR] Failed to activate virtual environment" -ForegroundColor Red
    Write-Host "  Run 'python -m venv venv' to create it first" -ForegroundColor Yellow
    exit 1
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
} else {
    Write-Host "[WARNING] No .env file found" -ForegroundColor Yellow
    Write-Host "  Copy env.example to .env and add your credentials" -ForegroundColor Gray
}
Write-Host ""

Write-Host "Ready! You can now run dbt commands." -ForegroundColor Green
Write-Host "Try: dbt debug (to test your connection)" -ForegroundColor Gray