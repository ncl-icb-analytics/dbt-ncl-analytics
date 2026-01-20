# build_changed.ps1
# Builds dbt models changed on the current branch vs base branch

param(
    [string]$Base = "main",     # Base branch to compare against
    [Alias("u")][switch]$Up,    # Include upstream dependencies (+model)
    [Alias("d")][switch]$Down,  # Include downstream dependents (model+)
    [Alias("r")][switch]$Run,   # dbt run (no tests)
    [Alias("t")][switch]$Test,  # dbt test (tests only)
    [switch]$DryRun
)

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

Push-Location $projectRoot

try {
    # Source start_dbt.ps1 to set up environment
    Write-Host "Setting up dbt environment..." -ForegroundColor Cyan
    . "$projectRoot\start_dbt.ps1"
    Write-Host ""

    # Get current branch
    $currentBranch = git rev-parse --abbrev-ref HEAD
    Write-Host "Current branch: $currentBranch" -ForegroundColor Cyan
    Write-Host "Base branch: $Base" -ForegroundColor Cyan
    Write-Host ""

    # Get merge base to find common ancestor
    $mergeBase = git merge-base $Base HEAD 2>$null
    if (-not $mergeBase) {
        Write-Host "[ERROR] Could not find common ancestor with $Base" -ForegroundColor Red
        exit 1
    }

    # Get changed SQL and YAML files in models/ directory
    # Include both committed changes (vs merge-base) and uncommitted working directory changes
    $committedSqlFiles = git diff --name-only $mergeBase HEAD -- "models/*.sql"
    $uncommittedSqlFiles = git diff --name-only HEAD -- "models/*.sql"
    $changedSqlFiles = ($committedSqlFiles + $uncommittedSqlFiles) | Sort-Object -Unique

    $committedYmlFiles = git diff --name-only $mergeBase HEAD -- "models/*.yml"
    $uncommittedYmlFiles = git diff --name-only HEAD -- "models/*.yml"
    $changedYmlFiles = ($committedYmlFiles + $uncommittedYmlFiles) | Sort-Object -Unique

    # Extract model names from SQL files
    $models = @()
    foreach ($file in $changedSqlFiles) {
        $models += [System.IO.Path]::GetFileNameWithoutExtension($file)
    }

    # Extract model names from YAML files by parsing model references
    foreach ($ymlFile in $changedYmlFiles) {
        if (Test-Path $ymlFile) {
            $content = Get-Content $ymlFile -Raw
            # Match only top-level model names (exactly 2 spaces before dash)
            # This avoids matching column names which have more indentation
            $ymlMatches = [regex]::Matches($content, '(?m)^  - name:\s+(\w+)')
            foreach ($match in $ymlMatches) {
                $models += $match.Groups[1].Value
            }
        }
    }

    if (-not $models) {
        Write-Host "No changed models found." -ForegroundColor Yellow
        exit 0
    }

    # Apply graph operators based on flags
    $modelsWithOperators = @()
    foreach ($modelName in $models) {
        if ($Up -and $Down) {
            $modelsWithOperators += "+$modelName+"
        } elseif ($Up) {
            $modelsWithOperators += "+$modelName"
        } elseif ($Down) {
            $modelsWithOperators += "$modelName+"
        } else {
            $modelsWithOperators += $modelName
        }
    }

    $models = $modelsWithOperators

    $uniqueModels = $models | Sort-Object -Unique
    $modelCount = $uniqueModels.Count

    Write-Host "Found $modelCount changed model(s):" -ForegroundColor Cyan
    foreach ($model in $uniqueModels) {
        Write-Host "  - $model" -ForegroundColor Gray
    }
    Write-Host ""

    # Build selector string
    $selector = ($uniqueModels | ForEach-Object { $_ }) -join " "

    # Determine command based on flags
    $command = if ($Test) { "test" } elseif ($Run) { "run" } else { "build" }

    if ($DryRun) {
        Write-Host "[DRY RUN] Would execute:" -ForegroundColor Yellow
        Write-Host "  dbt $command -s $selector" -ForegroundColor Gray
    } else {
        Write-Host "Running dbt $command..." -ForegroundColor Cyan
        Write-Host ""
        dbt $command -s $selector
    }
}
finally {
    Pop-Location
}
