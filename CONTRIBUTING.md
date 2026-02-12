# Contributing to NCL Analytics dbt Project

Welcome! This guide will help you get set up to contribute to this project.

## Before You Start

Make sure you have these prerequisites installed and configured on your Windows machine:

### 1. Install Required Software

- **uv** - Fast Python package manager (recommended)
  ```powershell
  powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
  ```
  uv handles Python installation automatically - no separate Python install needed.
- **Git for Windows** - [Download from git-scm.com](https://git-scm.com/download/win)
  - Minimum version 2.34 required for SSH commit signing
- **A text editor** - We recommend [VS Code](https://code.visualstudio.com/)
- **Access to Snowflake** with the ANALYST role

<details>
<summary>Alternative: Using pip (legacy method)</summary>

If you prefer pip over uv, install Python 3.8+ from [python.org](https://www.python.org/downloads/).
- **Important**: During installation, check "Add Python to PATH"
- If you forget, see troubleshooting below for how to add it manually

</details>

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
git clone https://github.com/ncl-icb-analytics/dbt-ncl-analytics
cd dbt-ncl-analytics
```

### Step 2: Set Up Python Environment

```bash
uv sync
.venv\Scripts\activate
```

This creates the virtual environment and installs all dependencies automatically.

<details>
<summary>Alternative: Using pip and venv (legacy method)</summary>

Create and activate a virtual environment:

```bash
python -m venv venv
venv\Scripts\activate
```

If the `python` command doesn't work, try `py -m venv venv` instead.

Install project dependencies:

```bash
pip install -r requirements.txt
```

</details>

**Important**: This project uses dbt-core 1.9.4 for Snowflake compatibility. Do not upgrade dbt packages.

### Step 3: Configure Snowflake Connection

```bash
cp env.example .env
```

Open `.env` in VS Code and fill in your Snowflake details:

```bash
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your.username
SNOWFLAKE_WAREHOUSE=ANALYST_WH
SNOWFLAKE_ROLE=ANALYST
```

The `profiles.yml` is already configured to read from your `.env` file.

### Step 4: Initialise Your Development Environment

Run the setup script:

```powershell
.\start_dbt.ps1
```

**Important**: Run this script each time you open a new terminal. It loads your `.env` credentials into the session - dbt commands won't work without it.

### Step 5: Verify Installation

```bash
dbt deps    # Install dbt packages
dbt debug   # Test connection
```

Your browser will open for Snowflake authentication. Look for "All checks passed!" in the output.

### Helper Scripts

Two scripts in the project root make development easier:

| Script | Description |
|--------|-------------|
| `.\start_dbt.ps1` | Loads `.env` credentials - **run first in each terminal** |
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
git switch -c feature/your-feature-name

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
   git push -u origin feature/your-feature-name
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
git switch feature/your-feature-name

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
git switch feature/your-feature-name

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

- Check existing [GitHub Issues](https://github.com/ncl-icb-analytics/dbt-ncl-analytics/issues)
- Review the [Development Guide](docs/development-guide.md) for advanced workflows
- Create a new issue with details about your problem

## Next Steps

Once you're set up:
1. Read the [Development Guide](docs/development-guide.md) for daily workflows
2. Review [Working with Sources](docs/working-with-sources.md) to understand the data pipeline
3. Check the README for project architecture and structure
