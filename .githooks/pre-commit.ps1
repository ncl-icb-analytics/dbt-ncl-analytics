# Pre-commit hook to prevent accidental commits of profiles.yml with credentials

# Check if profiles.yml is being committed
$stagedFiles = git diff --cached --name-only
if ($stagedFiles -match "^profiles\.yml$") {
    Write-Host "⚠️  WARNING: You're about to commit profiles.yml" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "This file should only be updated for Snowflake native execution changes," -ForegroundColor White
    Write-Host "not for local development credentials." -ForegroundColor White
    Write-Host ""
    Write-Host "If you really need to commit changes to profiles.yml:" -ForegroundColor Cyan
    Write-Host "  1. Ensure it contains NO credentials (empty strings for account/user)" -ForegroundColor Gray
    Write-Host "  2. Type 'yes' to continue, or anything else to abort" -ForegroundColor Gray
    Write-Host ""
    
    $response = Read-Host "Do you want to proceed? (yes/no)"
    
    if ($response -ne "yes") {
        Write-Host ""
        Write-Host "Commit aborted. Your changes are still staged." -ForegroundColor Red
        Write-Host ""
        Write-Host "To unstage profiles.yml: git reset HEAD profiles.yml" -ForegroundColor Gray
        Write-Host "To ignore local changes: git update-index --skip-worktree profiles.yml" -ForegroundColor Gray
        exit 1
    }
    
    Write-Host "Proceeding with commit. Please ensure no credentials are included!" -ForegroundColor Green
}