# Pre-commit hook to prevent accidental commits of credential files

# Check if any credential files are being committed
$credentialFiles = @(".env", ".env.local")
$stagedFiles = git diff --cached --name-only

foreach ($file in $credentialFiles) {
    if ($stagedFiles -contains $file) {
        Write-Host "⚠️  ERROR: You're about to commit credential file: $file" -ForegroundColor Red
        Write-Host ""
        Write-Host "This file should never be committed as it contains sensitive information." -ForegroundColor White
        Write-Host "Commit aborted." -ForegroundColor Red
        exit 1
    }
}