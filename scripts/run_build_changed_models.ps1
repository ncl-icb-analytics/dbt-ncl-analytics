# build_changed_models.ps1
# Builds all dbt models changed on the current branch vs base branch

param(
    [string]$BaseBranch = "main",
    [switch]$DryRun
)

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptRoot

Push-Location $projectRoot

try {
    # Source start_dbt.ps1 to set up environment
    Write-Host "Setting up dbt environment..." -ForegroundColor Cyan
    . .\start_dbt.ps1
    Write-Host ""

    # Get current branch
    $currentBranch = git rev-parse --abbrev-ref HEAD
    Write-Host "Current branch: $currentBranch" -ForegroundColor Cyan
    Write-Host "Base branch: $BaseBranch" -ForegroundColor Cyan
    Write-Host ""

    # Get merge base to find common ancestor
    $mergeBase = git merge-base $BaseBranch HEAD 2>$null
    if (-not $mergeBase) {
        Write-Host "[ERROR] Could not find common ancestor with $BaseBranch" -ForegroundColor Red
        exit 1
    }

    # Get changed SQL files in models/ directory only
    $changedFiles = git diff --name-only $mergeBase HEAD -- "models/*.sql"

    if (-not $changedFiles) {
        Write-Host "No changed models found." -ForegroundColor Yellow
        exit 0
    }

    # Convert file paths to model names
    $models = @()
    foreach ($file in $changedFiles) {
        # Extract model name from path (remove models/ prefix and .sql suffix)
        $modelName = [System.IO.Path]::GetFileNameWithoutExtension($file)
        $models += $modelName
    }

    $uniqueModels = $models | Sort-Object -Unique
    $modelCount = $uniqueModels.Count

    Write-Host "Found $modelCount changed model(s):" -ForegroundColor Cyan
    foreach ($model in $uniqueModels) {
        Write-Host "  - $model" -ForegroundColor Gray
    }
    Write-Host ""

    # Build selector string
    $selector = ($uniqueModels | ForEach-Object { $_ }) -join " "

    if ($DryRun) {
        Write-Host "[DRY RUN] Would execute:" -ForegroundColor Yellow
        Write-Host "  dbt build -s $selector" -ForegroundColor Gray
    } else {
        Write-Host "Running dbt build..." -ForegroundColor Cyan
        Write-Host ""
        dbt build -s $selector
    }
}
finally {
    Pop-Location
}