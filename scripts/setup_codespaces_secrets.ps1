# Upload Snowflake values from .env to personal GitHub Codespaces secrets for this repository.

$ErrorActionPreference = "Stop"

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "GitHub CLI (gh) is required. Install it and run 'gh auth login' first."
}

$envPath = Join-Path $PSScriptRoot "..\.env"
if (-not (Test-Path $envPath)) {
    throw "No .env file found at $envPath"
}

$originUrl = git remote get-url origin
if (-not $originUrl) {
    throw "Unable to determine git remote origin."
}

if ($originUrl -match 'github\.com[:/](?<repo>[^/]+/[^/.]+)(?:\.git)?$') {
    $repo = $matches['repo']
} else {
    throw "Could not parse GitHub repository from origin URL: $originUrl"
}

$allowedSecrets = @(
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_PAT",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_AUTHENTICATOR"
)

$secretValues = @{}
Get-Content $envPath | ForEach-Object {
    if ($_ -match '^\s*#' -or $_ -notmatch '=') {
        return
    }

    $name, $value = $_.Split('=', 2)
    $name = $name.Trim()
    $value = $value.Trim()

    if ($allowedSecrets -contains $name -and -not [string]::IsNullOrWhiteSpace($value)) {
        $secretValues[$name] = $value
    }
}

if ($secretValues.Count -eq 0) {
    throw "No supported Snowflake secrets were found in .env"
}

foreach ($entry in $secretValues.GetEnumerator()) {
    gh secret set $entry.Key --app codespaces --user --repos $repo --body $entry.Value | Out-Null
    Write-Host "Uploaded Codespaces secret $($entry.Key) for $repo"
}

Write-Host ""
Write-Host "Codespaces secrets are ready. Create a codespace with:"
Write-Host "  GitHub -> Code -> Codespaces -> New with options"
