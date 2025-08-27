  # start_dbt.ps1
  
  # Configure Git hooks for additional safety
  Write-Host "Configuring Git hooks..." -ForegroundColor Cyan
  $currentHooksPath = git config core.hooksPath
  
  if ($currentHooksPath -ne ".githooks") {
      git config core.hooksPath .githooks
      Write-Host "✓ Git hooks configured to use .githooks directory" -ForegroundColor Green
      Write-Host "  Pre-commit hook will warn before committing profiles.yml" -ForegroundColor Gray
  } else {
      Write-Host "✓ Git hooks already configured" -ForegroundColor Green
  }
  Write-Host ""
  
  # Ensure profiles.yml local changes are ignored by Git
  # This prevents accidental commits of local credentials
  Write-Host "Checking profiles.yml Git configuration..." -ForegroundColor Cyan
  $skipWorktreeStatus = git ls-files -v | Select-String -Pattern "^[sS] profiles\.yml"
  
  if (-not $skipWorktreeStatus) {
      Write-Host "Setting up Git to ignore local profiles.yml changes..." -ForegroundColor Yellow
      git update-index --skip-worktree profiles.yml 2>$null
      
      if ($LASTEXITCODE -eq 0) {
          Write-Host "✓ Git will now ignore your local profiles.yml changes" -ForegroundColor Green
      } else {
          Write-Host "Note: Could not set skip-worktree on profiles.yml (may not be tracked yet)" -ForegroundColor Gray
      }
  } else {
      Write-Host "✓ Git is already ignoring local profiles.yml changes" -ForegroundColor Green
  }
  
  Write-Host "  To update the repo's profiles.yml (for Snowflake native execution): git update-index --no-skip-worktree profiles.yml" -ForegroundColor Gray
  Write-Host ""
  
  # Check if profiles.yml needs local configuration
  Write-Host "Checking profiles.yml configuration..." -ForegroundColor Cyan
  $profilesContent = Get-Content "profiles.yml" -Raw
  
  # Check for empty account fields in any profile
  $emptyAccounts = @()
  
  # Check each line for profile names and empty accounts
  $lines = $profilesContent -split "`n"
  $currentProfile = ""
  
  foreach ($line in $lines) {
      # Check if this is a profile name (under outputs)
      if ($line -match "^\s{4}(dev|prod):") {
          $currentProfile = $Matches[1]
      }
      # Check if this line has an empty account field
      elseif ($line -match "^\s+account:\s*''\s*(\#|$)" -and $currentProfile) {
          $emptyAccounts += $currentProfile
          $currentProfile = ""  # Reset to avoid duplicate entries
      }
  }
  
  if ($emptyAccounts.Count -gt 0) {
      Write-Host "⚠ WARNING: The following profiles have empty credentials: $($emptyAccounts -join ', ')" -ForegroundColor Yellow
      Write-Host "  These are configured for Snowflake native execution (runs inside Snowflake)" -ForegroundColor Yellow
      Write-Host "  For local development, you need to add your Snowflake credentials:" -ForegroundColor Yellow
      Write-Host "    - account: Your Snowflake account" -ForegroundColor Gray
      Write-Host "    - user: Your username" -ForegroundColor Gray
      Write-Host "    - authenticator: externalbrowser (for SSO)" -ForegroundColor Gray
      Write-Host "  See profiles.yml.example for reference" -ForegroundColor Gray
      Write-Host ""
  } else {
      Write-Host "✓ profiles.yml appears to be fully configured" -ForegroundColor Green
      Write-Host ""
  }
  
  # Activate virtual environment
  Write-Host "Activating Python virtual environment..." -ForegroundColor Cyan
  & "venv\Scripts\Activate.ps1"
  
  if ($LASTEXITCODE -eq 0) {
      Write-Host "✓ Virtual environment activated" -ForegroundColor Green
      Write-Host "  Python: $(python --version 2>&1)" -ForegroundColor Gray
  } else {
      Write-Host "✗ Failed to activate virtual environment" -ForegroundColor Red
      Write-Host "  Run 'python -m venv venv' to create it first" -ForegroundColor Yellow
      exit 1
  }
  Write-Host ""

  # Load project-specific environment variables
  Write-Host "Loading environment variables from .env..." -ForegroundColor Cyan
  
  if (Test-Path ".env") {
      $envCount = 0
      Get-Content ".env" | ForEach-Object {
        if ($_ -match '^([^=]+)=(.*)$' -and -not $_.StartsWith('#')) {
            [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
            $envCount++
        }
      }
      Write-Host "✓ Loaded $envCount environment variables" -ForegroundColor Green
      
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
      Write-Host "⚠ No .env file found" -ForegroundColor Yellow
      Write-Host "  Copy env.example to .env and add your credentials" -ForegroundColor Gray
  }
  Write-Host ""
  
  Write-Host "Ready! You can now run dbt commands." -ForegroundColor Green
  Write-Host "Try 'dbt debug' to test your connection" -ForegroundColor Gray