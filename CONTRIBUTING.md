# Contributing to NCL Analytics DBT Project

When working with this project please remember this repository:
1. requires all commits to be signed
2. has branch protection rules in place to maintain code quality and security
3. uses pre-commit hooks that will automatically process and/or reject commits

## 1. SSH key setup for signing commits

### Setup
Before contributing to this project, you'll need to set up the following on your Windows machine.

1. **Generate an SSH key**:
   ```bash
   ssh-keygen -t ed25519 -C "your.email@nhs.net"
   ```
   - When prompted, press Enter to accept the default file location
   - Enter a passphrase (recommended). You will need to remember this passphrase and use it for commits.

2. **Configure Git to use SSH signing**:
   ```bash
   git config --global gpg.format ssh
   git config --global user.signingkey ~/.ssh/id_ed25519.pub
   git config --global commit.gpgsign true
   ```

3. **Add the SSH key to your GitHub account as a signing key**:
   - Copy your public key onto your clipboard: `Get-Content ~/.ssh/id_ed25519.pub | Set-Clipboard`
   - Go to GitHub Settings → SSH and GPG keys
   - Click "New SSH key"
   - Select "Signing Key" as the key type
   - Paste your public key and save

### Verifying Your Setup

1. **Create a test commit**:
   ```bash
   git commit --allow-empty -m "test: verify signed commits"
   ```

2. **Verify the signature**:
   ```bash
   git log --show-signature -1
   ```

3. **Check on GitHub**:
   - Push your commit
   - View on GitHub - it should show "Verified" badge

**If signing fails**:
- Ensure Git version is 2.34 or higher: `git --version`
- Update Git for Windows from https://git-scm.com/download/win if needed
- Verify the SSH key matches the one added to GitHub

## 2. Branch Protection Rules

The `main` branch is protected:
- **No direct commits**: All changes must go through a pull request
- **Signed commits required**: All commits must be cryptographically signed
- **Force pushes disabled**: History cannot be rewritten
- **Applies to everyone**: Including repository administrators

### Creating a Feature Branch

Never work directly on the main branch. Always create a new branch. This can be done following [VScode IDE guidance](https://code.visualstudio.com/docs/sourcecontrol/overview) or via git:

```bash
# Create and switch to a new feature branch
git switch -c feature/your-feature-name

# Or for bug fixes
git switch -c fix/your-bug-fix

# Or for documentation
git switch -c docs/your-doc-update
```

### Managing Work in Progress with Git Stash

If you need to switch branches but have uncommitted changes:

```bash
# Save current changes
git stash

# Switch to another branch
git switch main
git pull

# Return to your feature branch
git switch feature/your-feature-name

# Restore your changes
git stash pop
```

### Merging and Staying Up to Date

Keep your feature branch up to date:

```bash
# Update main
git switch main
git pull

# Merge into your feature branch
git switch feature/your-feature-name
git merge main
```

**If you encounter merge conflicts**:
1. Open the conflicting files and resolve conflicts (look for `<<<<<<<`, `=======`, `>>>>>>>` markers)
2. Stage the resolved files: `git add <file>`
3. Complete the merge: `git commit`

### Commit Message Format

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification. Commit messages should be structured as:

```
<type>: <description>

[optional body]

[optional footer(s)]
```

**Types**:
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that don't affect code meaning (formatting, etc.)
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding or correcting tests
- `chore`: Changes to build process or auxiliary tools
- `perf`: Performance improvements
- `ci`: Changes to CI configuration

**Examples**:
```bash
git commit -m "feat: add patient demographics staging model"
git commit -m "fix: correct join logic in int_appointments"
git commit -m "docs: update README with new setup instructions"
git commit -m "chore: update dbt dependencies to latest version"
```

### Creating a Pull Request

1. **Push your branch**:
   ```bash
   git push -u origin feature/your-feature-name
   ```

2. **Create a pull request on GitHub**:
   - Go to the repository on GitHub
   - Click "Pull requests" → "New pull request"
   - Select your branch
   - Fill in the PR template
   - Reference any related issues (e.g., "Fixes #123")

## 3. Pre-commit Hooks

Pre-commit hooks automatically validate and format your commits:
- Validates commit message format (Conventional Commits)
- Checks for trailing whitespace
- Ensures files end with a newline
- Fixes common formatting issues

If a hook fails, fix the issue and commit again.

## Windows-Specific Troubleshooting

**SSH connection issues**:
```bash
# Test connection
ssh -T git@github.com

# Check SSH agent
ssh-add -l

# Start SSH agent (PowerShell)
Start-Service ssh-agent
```

**Line ending issues**:
```bash
git config --global core.autocrlf true
```

## Environment Setup

See the main README for full setup instructions. Quick reference:

```bash
# Python virtual environment
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Snowflake credentials
cp env.example .env
# Edit .env with your credentials

# Run dbt setup script
.\start_dbt.ps1
```

*Note: If `python` command fails, use `py` instead or add Python to PATH.*

## Understanding start_dbt.ps1

The start_dbt.ps1 script:
1. Loads Snowflake credentials from .env into your session
2. Uses git skip-worktree to hide profiles.yml changes from git

Skip-worktree is a permanent local setting that persists across sessions and branches. Run this script once before your first commit.

## Working with dbt Packages and Profiles

Unlike typical dbt projects, this repository commits:
- `dbt_packages/` directory
- `profiles.yml` file

This is required for Snowflake native execution.

**Important**:
- Run `start_dbt.ps1` before your first commit to avoid committing credentials
- When `dbt deps` shows `dbt_packages/` changes, only commit if intentionally updating packages
- If you see `profiles.yml` in git status, run `start_dbt.ps1`

**To undo skip-worktree** (rarely needed):
```bash
git update-index --no-skip-worktree profiles.yml
```

## Getting Help

If you encounter issues:
- Check existing [GitHub Issues](https://github.com/ncl-icb-analytics/dbt-ncl-analytics/issues)
- Create a new issue with the appropriate label