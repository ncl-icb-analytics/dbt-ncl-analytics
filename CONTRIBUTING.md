# Contributing to WNL ICB Analytics dbt

This guide covers development setup and the pull request workflow. Use the
[dbt onboarding handbook](https://dbt-onboarding.vercel.app/) for training. Read
the [project conventions](PROJECT_CONVENTIONS.md) before changing a model.

## Prepare a local environment

You need:

- [Git for Windows](https://git-scm.com/download/win) 2.34 or later.
- Snowflake access with the `ANALYST` role.
- A text editor such as [Visual Studio Code](https://code.visualstudio.com/).
- PowerShell permission to run local scripts.

Python 3.11 and `uv` are optional. The project uses them only for helper scripts
in `scripts/`. dbt itself runs on the Fusion engine.

### Allow local PowerShell scripts

Run this command once:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

### Clone and configure the project

```powershell
git clone https://github.com/wnl-icb-analytics/dbt-analytics
Set-Location dbt-analytics
.\start_dbt.ps1
dbt deps
dbt debug
```

If `.env` does not exist, `start_dbt.ps1` asks for your Snowflake account,
username, warehouse, role, and authentication method. Browser SSO is the
default. You can also use a programmatic access token or a password with MFA.

The setup script installs or updates dbt Fusion, configures the Git hooks, and
syncs the optional Python tooling. The VS Code workspace runs it when you open a
terminal.

To configure Snowflake without the prompts, copy `env.example` to `.env` and
set the connection values:

```dotenv
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your.username
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_ROLE=ANALYST
```

For token authentication, also set `SNOWFLAKE_PAT`. For password authentication,
set `SNOWFLAKE_PASSWORD`. Do not commit `.env` or any credentials.

### Use GitHub Codespaces

Codespaces installs dbt Fusion, Python tooling, and dbt packages when it creates
the environment. Follow [Developing in GitHub Codespaces](docs/codespaces.md) to
configure Snowflake secrets and authentication.

## Sign commits

The repository requires signed commits. These steps configure SSH signing on
Windows.

1. Generate a key with the email address used by your GitHub account:

   ```powershell
   ssh-keygen -t ed25519 -C "your.email@nhs.net"
   ```

2. Create the allowed signers file. Replace the email address in the first
   command:

   ```powershell
   $signingEmail = "your.email@nhs.net"
   $publicKey = Get-Content "$env:USERPROFILE\.ssh\id_ed25519.pub"
   $signer = "$signingEmail $publicKey`n"
   [System.IO.File]::WriteAllText(
       "$env:USERPROFILE\.ssh\allowed_signers",
       $signer,
       [System.Text.UTF8Encoding]::new($false)
   )
   ```

3. Configure Git:

   ```powershell
   git config --global gpg.format ssh
   git config --global user.signingkey ~/.ssh/id_ed25519.pub
   git config --global commit.gpgsign true
   git config --global gpg.ssh.allowedSignersFile ~/.ssh/allowed_signers
   git config --global user.email "your.email@nhs.net"
   git config --global user.name "Your Name"
   ```

4. Copy the public key and add it to
   [GitHub SSH and GPG keys](https://github.com/settings/keys). Choose
   **Signing key**, not **Authentication key**.

   ```powershell
   Get-Content "$env:USERPROFILE\.ssh\id_ed25519.pub" | Set-Clipboard
   ```

5. On a temporary branch, create and inspect a signed test commit:

   ```powershell
   git commit --allow-empty -m "test: verify signed commits"
   git log --show-signature -1
   ```

The log output should report a good signature.

## Make a change

Never commit directly to `main`. Create a branch with a Conventional Commit
type as its prefix:

```powershell
git switch -c feat/short-description
# Other common prefixes: fix/, docs/, refactor/, test/, chore/
```

For model work:

1. Follow the [project conventions](PROJECT_CONVENTIONS.md).
2. Inspect the model contract, configuration, and lineage before editing.
3. Change the SQL, YAML description, owner, and contract tests together.
4. Check downstream impact and build the smallest useful selection.

```powershell
dbt ls -s model_name+
dbt compile -s model_name
dbt build -s model_name
# Run this when the change can affect downstream results.
dbt build -s model_name+
```

For several changed models, use `.\build_changed.ps1`. Add `-u` for upstream
dependencies, `-d` for downstream models, `-r` to skip tests, or `-t` to run
tests without models.

This repository is public. Follow the
[data-safety rules](PROJECT_CONVENTIONS.md#public-repository-data-safety) for
queries, test output, documentation, and pull requests.

## Commit the change

Use [Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>(optional-scope): <description>
```

Common types are `feat`, `fix`, `docs`, `refactor`, `test`, and `chore`.

```powershell
git commit -m "feat(olids): add a hypertension register"
git commit -m "fix(acute): preserve attendance grain"
git commit -m "docs: clarify local setup"
```

The pre-commit hooks check the commit message and basic file formatting. Fix a
reported problem and commit again. Do not bypass a hook without agreement from
a maintainer.

## Open a pull request

1. Push the branch:

   ```powershell
   git push -u origin HEAD
   ```

2. Open a pull request with a Conventional Commit title.
3. Start the description with the problem and why it matters. Then state the
   solution, validation, downstream limits, and any question for reviewers.
4. Link related issues, such as `Fixes #123`.
5. Resolve review comments and failed checks. The merge queue runs changed
   models and their data tests in the Snowflake development environment.

Before human review, fetch `origin/main` and check for conflicts:

```powershell
git fetch origin main
git merge origin/main
```

Do not force-push or rebase a shared branch without agreement from the other
contributors.

## Manage dbt packages

The repository commits `dbt_packages/` to keep package versions consistent.
Commit changes in that directory only when you intend to update a package.

## Troubleshoot setup

- If SSH signing fails, confirm that Git is version 2.34 or later and that the
  GitHub key type is **Signing key**.
- If PowerShell blocks the setup script, run the execution-policy command in
  [Allow local PowerShell scripts](#allow-local-powershell-scripts).
- If Snowflake authentication fails, check the values in `.env`, then run
  `dbt debug`.
- If a Python helper cannot find Python, install Python 3.11 and run `uv sync`.

For further help, search the
[existing issues](https://github.com/wnl-icb-analytics/dbt-analytics/issues) or
open an issue with the failing command, the safe part of its output, and the
expected result.
