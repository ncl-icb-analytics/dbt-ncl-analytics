# Contributing to NCL Analytics dbt Project

Welcome! This guide will help you get set up to contribute to this project.

## Before You Start

Make sure you have these prerequisites installed and configured on your Windows machine:

### 1. Install Required Software

- **Python 3.8 or higher** - [Download from python.org](https://www.python.org/downloads/)
  - **Important**: During installation, check "Add Python to PATH"
  - If you forget, see troubleshooting below for how to add it manually
- **Git for Windows** - [Download from git-scm.com](https://git-scm.com/download/win)
  - Minimum version 2.34 required for SSH commit signing
- **A text editor** - We recommend [VS Code](https://code.visualstudio.com/)
- **Access to Snowflake** with the ANALYST role

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

**Important**: This project uses dbt-core 1.9.4 for Snowflake compatibility. Do not upgrade dbt packages.

### Step 3: Configure Snowflake Connection

You need to create two configuration files:

#### 3a. Create .env file

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

#### 3b. Create profiles.yml file

```bash
cp profiles.yml.template profiles.yml
```

The template is pre-configured to read from your `.env` file, so no editing needed.

### Step 4: Initialise Your Development Environment

Run the setup script:

```powershell
.\start_dbt.ps1
```

This script:
- Loads your .env variables into the session
- Configures git to ignore local changes to profiles.yml

Run this script once before your first commit, then each time you open a new terminal session.

### Step 5: Verify Installation

```bash
dbt deps    # Install dbt packages
dbt debug   # Test connection
```

Your browser will open for Snowflake authentication. Look for "All checks passed!" in the output.

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

**2. Configure Git to use SSH signing:**

```bash
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/id_ed25519.pub
git config --global commit.gpgsign true
git config --global user.email "your.email@nhs.net"
git config --global user.name "Your Name"
```

**3. Add the SSH key to GitHub as a signing key:**

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

This repository commits `dbt_packages/` and `profiles.yml` (required for Snowflake native execution).

**Important:**
- The `start_dbt.ps1` script uses git skip-worktree to prevent committing your local `profiles.yml` changes
- When `dbt deps` shows changes in `dbt_packages/`, only commit if you're intentionally updating packages
- If you see `profiles.yml` in `git status`, run `start_dbt.ps1` again

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
- Ensure you're using `externalbrowser` authenticator in `profiles.yml`
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
