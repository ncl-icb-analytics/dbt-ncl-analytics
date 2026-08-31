# Working in dbt-analytics

This is a public NHS dbt project on Snowflake. Before model work, read and
follow [`PROJECT_CONVENTIONS.md`](PROJECT_CONVENTIONS.md). That file owns the
model, SQL, test, performance, validation, and review rules. This file adds
instructions for coding agents.

## Inspect before editing

- Confirm the requested outcome and the constraint behind it.
- Check the branch and worktree. Preserve unrelated changes and never work on
  `main`.
- Inspect related contracts, configuration, lineage, and downstream consumers.
- Start from the most downstream established model whose contract fits. Move
  upstream only when that contract is insufficient.
- Check nearby models before adding local configuration. Folder configuration in
  `dbt_project.yml` controls databases, schemas, materialisations, tags, hooks,
  grants, and documentation.

Prefer the smallest coherent design. Reuse a suitable contract, but remove
needless complexity when the change makes that practical.

## Manage scope and decisions

Make routine, reversible choices without asking the user. Raise a concern
before following a direction likely to produce wrong results, duplicate a
pipeline, obscure a contract, or add avoidable cost. State the consequence and
offer the smallest practical alternative.

Ask the user before you:

- Choose or change a clinical or business definition without clear authority.
- Change an analyst-facing contract or materially widen the scope.
- Take an action that is difficult to reverse.

Many warehouse domains do not yet have a dbt model. For a new domain, consider
whether it needs a durable, reusable entity at an explicit grain. Do not hide
shared domain logic in a report-specific pipeline or copy every source column.
If the reusable design would widen the request, explain the option and agree the
boundary with the user.

## Implement the change

- Keep SQL, YAML descriptions, business ownership, and contract tests together.
- Write SQL comments only for non-obvious meaning, source quirks, or surprising
  choices. Do not narrate the SQL.
- Write descriptions as short analyst-facing contracts. Document the subject,
  grain, population, time basis, units, codes, and null meaning where they affect
  interpretation.
- Call out changes to `dbt_project.yml` because they can affect many models.
- Use synthetic examples. Never place credentials or real patient-level or
  person-level data in repository files.

## Validate safely

Check downstream impact, then compile and build the smallest useful selection:

```powershell
dbt ls -s model_name+
dbt compile -s model_name
dbt build -s model_name
# Run when downstream results may change.
dbt build -s model_name+
```

Assume that an agent's command output is visible to its provider. Use `dbt show`
only for a query designed to return a high-level, non-identifying aggregate.
Never use it to preview model rows. For row-level validation, provide a
Snowflake-native query for a human-controlled session approved for patient-level
data, then ask only for the safe aggregate or confirmation needed.

Read the final diff and worktree status before pushing. Report any validation
that you could not run.

## Write commits and pull requests

Use a `type/short-description` branch and Conventional Commit form for commits
and pull request titles. Open the pull request description with the problem and
why it matters. Then give the solution, validation, and review focus. Do not add
attribution to an agent, language model, or execution harness.

Before human review, fetch and check against `origin/main`. Report conflicts.
Do not rebase or force-push a shared branch without the user's approval.

## Review pull requests

Review changed behaviour and its effect on the existing contract. Verify
automated findings against the diff and source. Fix genuine issues. Answer an
incorrect finding with a brief reason rather than changing correct code to
satisfy a bot.

When asked to monitor a pull request, inspect checks and comments posted after
the latest push. Draft pull requests are acceptable while work or decisions
remain.

## Protect public data

Never include credentials or real patient-level or person-level data in files,
GitHub text, seeds, test data, queries, logs, errors, screenshots, or examples.
Do not repeat a suspected sensitive value. Identify its location and data
category, remove it, and alert the repository maintainers because deleting the
latest diff does not remove public history.

High-level aggregates are acceptable only when they cannot identify a person.
The full rules are in
[`PROJECT_CONVENTIONS.md`](PROJECT_CONVENTIONS.md#public-repository-data-safety).
