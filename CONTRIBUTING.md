# Contributing to WNL ICB Analytics dbt Project

Welcome! This guide will help you get set up to contribute to this project.

## Before You Start

Make sure you have these prerequisites installed and configured on your Windows machine:

### 1. Install Required Software

- **dbt Fusion engine** - runs all dbt commands. `start_dbt.ps1` installs and keeps
  it up to date automatically (to `%USERPROFILE%\.local\bin`), so you normally don't
  install it by hand. To install manually:
  ```powershell
  irm https://public.cdn.getdbt.com/fs/install/install.ps1 | iex
  ```
  dbt is **not** a Python package in this project - it is the Fusion binary.
- **Git for Windows** - [Download from git-scm.com](https://git-scm.com/download/win)
  - Minimum version 2.34 required for SSH commit signing
- **A text editor** - We recommend [VS Code](https://code.visualstudio.com/)
- **Access to Snowflake** with the ANALYST role
- **uv** *(optional)* - only needed to run the Python helper scripts in `scripts/`:
  ```powershell
  powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
  ```

### 2. Enable PowerShell Script Execution

Open PowerShell and run:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

This allows the project's setup script (`start_dbt.ps1`) to run.

### 3. Get Your Snowflake Connection Details

You'll need the following information from Snowflake (ask your team lead if you don't have access):

**To find your connection details in Snowflake:**
1. Log in to Snowflake web interface
2. Click your user/role name in the bottom-left corner
3. Select "Connect a tool to Snowflake"
4. You'll see your account identifier and other connection details

**You'll need:**
- **Account identifier** - Shown in the connection dialog
- **Username** - Your Snowflake username (usually your email prefix)
- **Warehouse** - Usually `NCL_ANALYTICS_XS`
- **Role** - `ANALYST`

## Getting Started

### Step 1: Clone the Repository

```bash
git clone https://github.com/wnl-icb-analytics/dbt-analytics
cd dbt-analytics
```

### Step 2: Install dbt + Python tooling

Just run the setup script (Step 4) - it installs the dbt Fusion engine and syncs
the Python tooling for you. To do it by hand:

```powershell
# dbt Fusion engine (runs all dbt commands)
irm https://public.cdn.getdbt.com/fs/install/install.ps1 | iex

# Python tooling for scripts/ (optional)
uv sync
.venv\Scripts\activate
```

dbt runs on the Fusion engine, not from the Python venv. The `.venv` exists only
for the helper scripts in `scripts/`.

### Step 3: Configure Snowflake Connection

The first time you open a terminal with no `.env`, `start_dbt.ps1` walks you through
setup interactively: it asks for your account, user, role and warehouse, then your
auth method (browser SSO by default, or PAT / password+MFA), and writes `.env` for you.

To configure it by hand instead:

```bash
cp env.example .env
```

Then fill in `.env`:

```bash
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your.username
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_ROLE=your-role
```

Auth: leave it there for **browser SSO** (the default). For a **PAT**, set `SNOWFLAKE_PAT`
(Fusion authenticates via `programmatic_access_token`). For an **account password**, set
`SNOWFLAKE_PASSWORD` (used with MFA). `profiles.yml` picks the authenticator from whichever
you set.

### Step 4: Initialise Your Development Environment

Run the setup script:

```powershell
.\start_dbt.ps1
```

The VS Code workspace runs this automatically when you open a terminal, so you
rarely need to run it by hand. It installs/updates the dbt Fusion engine, configures
git hooks, and syncs the Python tooling. (Fusion loads `.env` itself, so dbt works
even if the script hasn't run.)

### Step 5: Verify Installation

```bash
dbt deps    # Install dbt packages
dbt debug   # Test connection
```

If you are using `externalbrowser`, your browser will open for Snowflake authentication. Look for "All checks passed!" in the output.

## GitHub Codespaces

Codespaces should use GitHub Codespaces secrets for Snowflake credentials. Do not commit credentials into the repository.

### Recommended Codespaces secrets

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_PAT`

`SNOWFLAKE_PAT` is the preferred Codespaces auth method because it is non-interactive and each developer can create their own token in Snowflake.

### Create Codespaces secrets from an existing local `.env`

If you already have a working local `.env`, you can upload the supported Snowflake values to your personal Codespaces secrets for this repository:

```powershell
.\scripts\setup_codespaces_secrets.ps1
```

This uses GitHub CLI to create user-level Codespaces secrets scoped to this repository.

### Create a codespace

1. In GitHub, open the repository and choose **Code** -> **Codespaces** -> **New with options**.
2. Confirm the recommended secrets if prompted.
3. Create the codespace on this branch.
4. Wait for the dev container to finish setup.
5. Run `dbt debug` to confirm the Snowflake connection.

### How credentials work inside Codespaces

- Codespaces secrets are exposed as environment variables in the running codespace.
- You do not need a `.env` file inside the codespace if the required secrets are already set.
- The setup scripts will use existing environment variables before creating a `.env` template.

### Helper Scripts

Two scripts in the project root make development easier:

| Script | Description |
|--------|-------------|
| `.\start_dbt.ps1` | Installs/updates dbt Fusion, configures git hooks, loads `.env`, syncs Python tooling (auto-runs on terminal open) |
| `.\build_changed` | Builds only models changed on your branch |

**build_changed flags:**
- `-u` include upstream dependencies
- `-d` include downstream dependents
- `-r` run only (skip tests)
- `-t` test only (skip run)

Example: `.\build_changed -u -d` builds changed models with all dependencies.

## Setting Up Commit Signing

This repository requires all commits to be cryptographically signed.

### Why Sign Commits?

Commit signing proves that commits actually came from you, not someone impersonating you. GitHub will show a "Verified" badge on signed commits.

### Setup Process

**1. Generate an SSH key:**

```bash
ssh-keygen -t ed25519 -C "your.email@nhs.net"
```

**Important**: Use the same email address that you use for your GitHub account.

- Press Enter to accept the default file location (`~/.ssh/id_ed25519`)
- Enter a passphrase when prompted (recommended for security)

**2. Create an allowed signers file (for local signature verification):**

```bash
echo "your.email@nhs.net $(cat ~/.ssh/id_ed25519.pub)" > ~/.ssh/allowed_signers
```

**3. Configure Git to use SSH signing:**

```bash
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/id_ed25519.pub
git config --global commit.gpgsign true
git config --global gpg.ssh.allowedSignersFile ~/.ssh/allowed_signers
git config --global user.email "your.email@nhs.net"
git config --global user.name "Your Name"
```

**4. Add the SSH key to GitHub as a signing key:**

Copy your public key:
```bash
Get-Content ~/.ssh/id_ed25519.pub | Set-Clipboard
```

Then:
1. Go to [GitHub Settings → SSH and GPG keys](https://github.com/settings/keys)
2. Click "New SSH key"
3. **Important**: Select "Signing Key" as the key type (not "Authentication Key")
   - There's a dropdown that defaults to "Authentication Key"
   - You must change this to "Signing Key"
4. Paste your public key and give it a descriptive title (e.g., "Work Laptop Signing Key")
5. Click "Add SSH key"

### Verify Your Setup

Create a test commit:

```bash
git commit --allow-empty -m "test: verify signed commits"
```

Fix line-endings using this command (thx Kate)

```bash
$file = "$env:USERPROFILE\.ssh\allowed_signers"; $content = [System.IO.File]::ReadAllText($file); [System.IO.File]::WriteAllText($file, $content.Replace("`r`n", "`n"), [System.Text.Encoding]::UTF8); Write-Host "Line endings converted from CRLF to LF"
```

Check the signature:

```bash
git log --show-signature -1
```

You should see "Good signature" in the output.

## Development Workflow

### Branch Protection Rules

The `main` branch is protected:
- **No direct commits** - All changes must go through a pull request
- **Signed commits required** - All commits must be signed
- **No force pushes** - History cannot be rewritten

### Creating a Feature Branch

Never work directly on main. Always create a new branch:

```bash
# Create and switch to a new feature branch
git switch -c feat/your-feature-name

# Or for bug fixes
git switch -c fix/your-bug-fix

# Or for documentation
git switch -c docs/your-doc-update
```

### Commit Message Format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>: <description>

[optional body]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding or correcting tests
- `chore`: Changes to build process or tools

**Examples:**
```bash
git commit -m "feat: add patient demographics staging model"
git commit -m "fix: correct join logic in int_appointments"
git commit -m "docs: update setup instructions in CONTRIBUTING"
```

### Creating a Pull Request

1. **Push your branch:**
   ```bash
   git push -u origin feat/your-feature-name
   ```

   The `-u origin branch-name` creates the branch on GitHub and links it to your local branch. After this first push, you can use just `git push` for subsequent updates.

2. **Create PR on GitHub:**
   - Go to the repository on GitHub
   - Click "Pull requests" → "New pull request"
   - Select your branch
   - Fill in the PR description
   - Reference any related issues (e.g., "Fixes #123")

3. **Wait for review:**
   - Pre-commit hooks will automatically run
   - Address any feedback from reviewers
   - Once approved, the PR can be merged

### Keeping Your Branch Up to Date

```bash
# Switch to main and pull latest changes
git switch main
git pull

# Switch back to your feature branch
git switch feat/your-feature-name

# Merge main into your branch
git merge main
```

If you encounter merge conflicts, Git will tell you which files have conflicts. Open those files, look for conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`), resolve them, then:

```bash
git add <resolved-files>
git commit
```

### Using Git Stash

If you need to switch branches but have uncommitted changes:

```bash
# Save your current work
git stash

# Switch branches and do other work
git switch main
git pull

# Go back to your feature branch
git switch feat/your-feature-name

# Restore your saved changes
git stash pop
```

## Pre-commit Hooks

Pre-commit hooks run automatically when you commit and will:
- Validate commit message format
- Check for trailing whitespace
- Ensure files end with newlines
- Fix common formatting issues

If a hook fails, fix the reported issue and commit again.

## Working with dbt Packages

This repository commits `dbt_packages/` to ensure consistent package versions. When `dbt deps` shows changes in `dbt_packages/`, only commit if you're intentionally updating packages.

## Common Issues

**SSH signing fails:**
- Check Git version: `git --version` (need 2.34+)
- Verify SSH key matches the one on GitHub
- Make sure you selected "Signing Key" not "Authentication Key"

**Python command not found:**
- Use `py` instead of `python`
- Or add Python to PATH (see README)

**PowerShell won't run scripts:**
- Run `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`

**dbt authentication fails:**
- Check your `.env` file has correct values
- Try running `dbt debug` to see detailed error

## Getting Help

- Check existing [GitHub Issues](https://github.com/wnl-icb-analytics/dbt-analytics/issues)
- Review the [Development Guide](docs/development-guide.md) for advanced workflows
- Create a new issue with details about your problem

## Next Steps

Once you're set up:
1. Read the [Modelling Guide](docs/modelling-guide.md) to understand the layers, naming conventions, and how to build models
2. Read the [Materialisation Guide](docs/materialisation-guide.md) to understand views, tables, incremental models, and ephemeral CTEs
3. Review [Working with Sources](docs/working-with-sources.md) to understand the source generation pipeline
4. Read the [Development Guide](docs/development-guide.md) for daily workflows and advanced patterns
