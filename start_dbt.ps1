# start_dbt.ps1
#
# Bootstraps the dev environment for this project. Auto-runs when you open a
# terminal in the VS Code workspace (see dbt-ncl-analytics.code-workspace).
#
# What it does:
#   1. Configures git hooks + checks commit signing
#   2. Installs / updates the dbt Fusion engine (dbt runs on Fusion, not Python)
#   3. Clears stale dbt-core artifacts if a legacy setup is detected
#   4. Syncs the Python venv used only by the helper scripts in scripts/
#   5. Loads .env and installs dbt packages
#
# dbt itself is the Fusion binary (installed to %USERPROFILE%\.local\bin), NOT a
# Python package. The .venv exists only for the Python tooling in scripts/.

# Fusion engine version. Pinned (via .fusion-version at repo root) to the version
# Snowflake hosts, so local, CI, and the 5am native build run the same engine.
# Bump .fusion-version when Snowflake's hosted 2.0.0-preview moves.
# Set $FusionVersionPin to override locally; fallback is used only if the file is missing.
$FusionVersionPin = ''
$FusionFallbackVersion = '2.0.0-preview.186'

$actions = @()
$installDir = Join-Path $env:USERPROFILE '.local\bin'
# Installer platform target. The installer's auto-detection is currently
# unreliable, so we pass it (and the version) explicitly.
$arch = if ($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') { 'aarch64' } else { 'x86_64' }
$fusionTarget = "$arch-pc-windows-msvc"

# Ensure the Fusion install dir is on PATH for this session.
if ($env:PATH -notlike "*$installDir*") {
    $env:PATH = "$installDir;$env:PATH"
}

# ---------------------------------------------------------------------------
# 1. Git hooks
# ---------------------------------------------------------------------------
Write-Host "Configuring Git hooks..." -ForegroundColor Cyan
if ((git config core.hooksPath) -ne ".githooks") {
    git config core.hooksPath .githooks
    Write-Host "[OK] Git hooks configured to use .githooks directory" -ForegroundColor Green
} else {
    Write-Host "[OK] Git hooks already configured" -ForegroundColor Green
}
Write-Host ""

# ---------------------------------------------------------------------------
# 2. Commit signing check
# ---------------------------------------------------------------------------
Write-Host "Checking commit signing..." -ForegroundColor Cyan
$gpgFormat = git config gpg.format
$signingKey = git config user.signingkey
$autoSign = git config commit.gpgsign
if ($gpgFormat -eq "ssh" -and $signingKey -and $autoSign -eq "true") {
    Write-Host "[OK] Commit signing configured" -ForegroundColor Green
} else {
    Write-Host "[WARNING] Commit signing is not configured" -ForegroundColor Yellow
    Write-Host "  This repository requires signed commits for branch protection." -ForegroundColor Gray
    Write-Host "  See CONTRIBUTING.md 'Setting Up Commit Signing' for setup instructions." -ForegroundColor Gray
    $actions += "Set up commit signing (see CONTRIBUTING.md)"
}
Write-Host ""

# ---------------------------------------------------------------------------
# 3. dbt Fusion engine
# ---------------------------------------------------------------------------
Write-Host "Checking dbt Fusion engine..." -ForegroundColor Cyan

function Resolve-FusionVersion {
    if ($FusionVersionPin) { return $FusionVersionPin }
    $pinFile = Join-Path $PSScriptRoot '.fusion-version'
    if (Test-Path $pinFile) {
        $v = (Get-Content $pinFile -Raw).Trim()
        if ($v) { return $v }
    }
    return $FusionFallbackVersion
}

function Install-Fusion {
    param([string]$Version, [switch]$Update)
    # Clear partial downloads left by a previous failed/locked install, else the
    # installer can trip over them.
    Get-ChildItem -Path $installDir -Filter 'tmp-dbt-download-*' -Directory -ErrorAction SilentlyContinue |
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    $installer = [scriptblock]::Create((Invoke-RestMethod 'https://public.cdn.getdbt.com/fs/install/install.ps1'))
    if ($Update) { & $installer -Version $Version -Target $fusionTarget -Update }
    else { & $installer -Version $Version -Target $fusionTarget }
}

try {
    $dbtPresent = [bool](Get-Command dbt -ErrorAction SilentlyContinue)
    # Resolution is a local file read, so check every launch and (re)install only on mismatch.
    $desired = Resolve-FusionVersion
    $current = if ($dbtPresent) { (dbt --version 2>&1) -join ' ' } else { '' }
    if ($current -notmatch [regex]::Escape($desired)) {
        Write-Host "[INFO] Installing dbt Fusion $desired..." -ForegroundColor Cyan
        Install-Fusion -Version $desired -Update:$dbtPresent
        if ($env:PATH -notlike "*$installDir*") { $env:PATH = "$installDir;$env:PATH" }
    } else {
        Write-Host "[OK] dbt Fusion $desired" -ForegroundColor Green
    }
    Write-Host "  $((dbt --version 2>&1) | Select-Object -First 1)" -ForegroundColor Gray
} catch {
    Write-Host "[WARNING] Could not install/update dbt Fusion: $_" -ForegroundColor Yellow
    if ("$_" -match 'EPERM|rename|being used|denied|Access') {
        # dbt.exe is locked - usually a running dbt LSP (the VS Code dbt extension).
        Write-Host "  dbt.exe looks locked by a running process (often the VS Code dbt extension's LSP)." -ForegroundColor Gray
        Write-Host "  Close VS Code, then run: Get-Process dbt -ErrorAction SilentlyContinue | Stop-Process -Force" -ForegroundColor Gray
        Write-Host "  and re-open the terminal. Existing dbt keeps working in the meantime." -ForegroundColor Gray
    } else {
        Write-Host "  Install manually: irm https://public.cdn.getdbt.com/fs/install/install.ps1 | iex" -ForegroundColor Gray
    }
    $actions += "Install dbt Fusion (see CONTRIBUTING.md)"
}
Write-Host ""

# ---------------------------------------------------------------------------
# 4. Legacy dbt-core cleanup
# ---------------------------------------------------------------------------
# A dbt-core install puts a dbt entrypoint in .venv\Scripts. Fusion does not
# (it lives in %USERPROFILE%\.local\bin), so this marks a pre-Fusion machine.
if (Test-Path ".venv\Scripts\dbt.exe") {
    Write-Host "Clearing legacy dbt-core artifacts..." -ForegroundColor Cyan
    foreach ($d in @("target", "logs")) {
        if (Test-Path $d) { Remove-Item -Recurse -Force $d; Write-Host "  Removed $d/" -ForegroundColor Gray }
    }
    Write-Host "  dbt-core will be pruned from .venv by uv sync below" -ForegroundColor Gray
    Write-Host ""
}

# ---------------------------------------------------------------------------
# 5. Python venv for the helper scripts in scripts/ (optional - not needed for dbt)
# ---------------------------------------------------------------------------
Write-Host "Syncing Python tooling for scripts/..." -ForegroundColor Cyan
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Host "[INFO] uv not found - installing..." -ForegroundColor Cyan
    try { Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression } catch { Write-Host "[WARNING] uv install failed: $_" -ForegroundColor Yellow }
    $uvBin = Join-Path $env:USERPROFILE '.local\bin'
    if ($env:PATH -notlike "*$uvBin*") { $env:PATH = "$uvBin;$env:PATH" }
}
if (Get-Command uv -ErrorAction SilentlyContinue) {
    uv sync
    if ($LASTEXITCODE -eq 0) {
        if (Test-Path ".venv\Scripts\Activate.ps1") { & ".venv\Scripts\Activate.ps1" }
        Write-Host "[OK] Python tooling ready" -ForegroundColor Green
    } else {
        Write-Host "[WARNING] uv sync failed - scripts/ Python tools may be incomplete" -ForegroundColor Yellow
        $actions += "Re-run 'uv sync' for the Python helper scripts"
    }
} else {
    Write-Host "[WARNING] uv unavailable - scripts/ Python tools skipped" -ForegroundColor Yellow
    $actions += "Install uv to run the Python helper scripts (optional)"
}
Write-Host ""

# Disable AWS metadata service checks (prevents connection pool warnings on Azure)
[System.Environment]::SetEnvironmentVariable('AWS_EC2_METADATA_DISABLED', 'true', 'Process')

function Read-HostSecret {
    param([string]$Prompt)
    $sec = Read-Host $Prompt -AsSecureString
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)
    try { return [Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr) }
    finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}

# Interactive first-run setup: prompt for connection details and write .env.
function New-DbtEnvFile {
    param([string]$Path = ".env")
    Write-Host ""
    Write-Host "No .env found - let's set up your Snowflake connection." -ForegroundColor Cyan
    Write-Host "(Press Enter to accept [defaults].)" -ForegroundColor Gray

    $account = Read-Host "Snowflake account identifier"
    $user = Read-Host "Snowflake username"
    $role = Read-Host "Snowflake role"
    $warehouse = Read-Host "Snowflake warehouse"

    Write-Host ""
    Write-Host "Authentication method:" -ForegroundColor Cyan
    Write-Host "  1) Browser SSO (externalbrowser)   [default, recommended]" -ForegroundColor Gray
    Write-Host "  2) Programmatic Access Token (PAT)" -ForegroundColor Gray
    Write-Host "  3) Account password + MFA" -ForegroundColor Gray
    Write-Host "     (PAT -> SNOWFLAKE_PAT/token; password -> SNOWFLAKE_PASSWORD)" -ForegroundColor DarkGray
    $choice = Read-Host "Selection [1]"
    if (-not $choice) { $choice = '1' }

    # Only write keys the user actually provided - no assumed defaults.
    $lines = @("# Generated by start_dbt.ps1 onboarding. Never commit this file.")
    if ($account)   { $lines += "SNOWFLAKE_ACCOUNT=$account" }
    if ($user)      { $lines += "SNOWFLAKE_USER=$user" }
    if ($role)      { $lines += "SNOWFLAKE_ROLE=$role" }
    if ($warehouse) { $lines += "SNOWFLAKE_WAREHOUSE=$warehouse" }
    switch ($choice) {
        '2' { $lines += "SNOWFLAKE_PAT=$(Read-HostSecret 'Paste your PAT')" }
        '3' {
            $lines += "SNOWFLAKE_PASSWORD=$(Read-HostSecret 'Account password')"
            $lines += "SNOWFLAKE_AUTHENTICATOR=username_password_mfa"
        }
        default { $lines += "SNOWFLAKE_AUTHENTICATOR=externalbrowser" }
    }

    Set-Content -Path $Path -Value $lines -Encoding utf8
    # Load into the current session so dbt works immediately.
    foreach ($l in $lines) {
        if ($l -match '^([^#][^=]*)=(.*)$') {
            [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
        }
    }
    Write-Host "[OK] Created $Path" -ForegroundColor Green
}

# ---------------------------------------------------------------------------
# 6. Load environment variables from .env
# ---------------------------------------------------------------------------
# Fusion auto-loads .env, but we also load it here for the Python scripts and to
# surface the active connection details.
$hasSnowflakeEnv = [bool]($env:SNOWFLAKE_ACCOUNT -and $env:SNOWFLAKE_USER -and ($env:SNOWFLAKE_PAT -or $env:SNOWFLAKE_PASSWORD -or $env:SNOWFLAKE_AUTHENTICATOR -eq "externalbrowser"))

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

    $hasPlaceholders = (Get-Content $envPath | Where-Object { $_ -match '^[^#].*=.*your-.*-here' } | Measure-Object).Count -gt 0
    if ($hasPlaceholders) {
        Write-Host "[WARNING] .env still contains placeholder values" -ForegroundColor Yellow
        $actions += 'Update credentials in .env, then open a new terminal'
    } else {
        if ($env:SNOWFLAKE_ACCOUNT) {
            $accountPrefix = $env:SNOWFLAKE_ACCOUNT.Substring(0, [Math]::Min(10, $env:SNOWFLAKE_ACCOUNT.Length))
            Write-Host "  SNOWFLAKE_ACCOUNT: $accountPrefix..." -ForegroundColor Gray
        }
        if ($env:SNOWFLAKE_USER) { Write-Host "  SNOWFLAKE_USER: $env:SNOWFLAKE_USER" -ForegroundColor Gray }
        if ($env:SNOWFLAKE_ROLE) { Write-Host "  SNOWFLAKE_ROLE: $env:SNOWFLAKE_ROLE" -ForegroundColor Gray }
        if ($env:SNOWFLAKE_WAREHOUSE) { Write-Host "  SNOWFLAKE_WAREHOUSE: $env:SNOWFLAKE_WAREHOUSE" -ForegroundColor Gray }
        if ($env:SNOWFLAKE_PAT) {
            $patPrefix = $env:SNOWFLAKE_PAT.Substring(0, [Math]::Min(8, $env:SNOWFLAKE_PAT.Length))
            Write-Host "  SNOWFLAKE_PAT: $patPrefix..." -ForegroundColor Gray
        }
        if ($env:SNOWFLAKE_PASSWORD) { Write-Host "  SNOWFLAKE_PASSWORD: [set]" -ForegroundColor Gray }
        if ($env:SNOWFLAKE_AUTHENTICATOR) { Write-Host "  SNOWFLAKE_AUTHENTICATOR: $env:SNOWFLAKE_AUTHENTICATOR" -ForegroundColor Gray }
    }
} elseif ($hasSnowflakeEnv) {
    Write-Host "[OK] No .env file found - using existing environment variables" -ForegroundColor Green
} elseif (-not [Console]::IsInputRedirected) {
    # Interactive terminal with no .env - walk the user through setup.
    New-DbtEnvFile
} elseif (Test-Path "env.example") {
    # Non-interactive (e.g. automation) - fall back to the template.
    Copy-Item "env.example" ".env"
    Write-Host "[WARNING] No .env file found - created from template" -ForegroundColor Yellow
    $actions += 'Update credentials in .env, then open a new terminal'
} else {
    Write-Host "[WARNING] No .env file found and no env.example template" -ForegroundColor Yellow
    $actions += 'Update credentials in .env, then open a new terminal'
}
Write-Host ""

# ---------------------------------------------------------------------------
# 7. dbt packages - install if missing, or if packages.yml changed since last install
# ---------------------------------------------------------------------------
# Drop a stale dbt-core-format package-lock.yml. Fusion flags it (dbt1041 "Old
# format package-lock.yml") and its pins can clash with packages.yml (dbt1005).
# The old format lacks the per-package `name:` field Fusion writes, so detect by
# its absence; `dbt deps` then regenerates a current, in-sync lock.
$lockRemoved = $false
if ((Test-Path "package-lock.yml") -and -not (Select-String -Path "package-lock.yml" -Pattern '^\s*name:' -Quiet)) {
    Remove-Item "package-lock.yml" -Force
    Write-Host "[INFO] Removed old-format package-lock.yml - dbt deps will regenerate it" -ForegroundColor Cyan
    $lockRemoved = $true
}
$needDeps = $lockRemoved -or -not (Test-Path "dbt_packages")
if (-not $needDeps -and (Test-Path "packages.yml")) {
    if ((Get-Item "packages.yml").LastWriteTimeUtc -gt (Get-Item "dbt_packages").LastWriteTimeUtc) {
        $needDeps = $true
    }
}
if ($needDeps) {
    Write-Host "Installing dbt packages (dbt deps)..." -ForegroundColor Cyan
    try { dbt deps } catch { $actions += "Run 'dbt deps' to install dbt packages" }
    Write-Host ""
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
if ($actions.Count -gt 0) {
    Write-Host "To finish setup:" -ForegroundColor Yellow
    foreach ($action in $actions) {
        Write-Host "  - $action" -ForegroundColor Gray
    }
} else {
    Write-Host "Ready! dbt runs on the Fusion engine." -ForegroundColor Green
    Write-Host "Try: dbt debug" -ForegroundColor Gray
}
