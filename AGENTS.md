# Working in dbt-analytics

This is a public NHS dbt project on Snowflake. Read
`PROJECT_CONVENTIONS.md` before model work.

Inspect the intended outcome, real constraint, related contracts, configuration
and lineage before editing. Prefer the smallest coherent design, not the
smallest diff or narrowest answer. Reuse settled contracts, but do not preserve
complexity merely because it exists.

Many warehouse healthcare objects are not yet represented in dbt. For a new
domain, model generally useful, durable entities or concepts at an explicit
grain. Include fields and shared definitions with plausible analytical use; do
not bury shared domain logic in a report-specific pipeline or copy every source
column. If this would materially widen the request, explain the opportunity and
agree the boundary with the user.

Use these principles when reasoning about every change:

- Measure twice, cut once: inspect contracts, lineage and consequences first.
- YAGNI: reject imagined machinery, not reusable domain modelling.
- KISS and simple design: prefer readable SQL and obvious contracts.
- DRY and the Rule of Three: centralise stable business definitions; abstract
  implementation patterns only after they prove stable.
- Make it work, make it right, make it fast: establish the correct contract,
  clarify the design, then tune a plausible or demonstrated cost.

Raise a concern before implementing a direction likely to cause wrong results,
an unclear contract, a duplicate pipeline or avoidable cost. State the
consequence and offer the smallest realistic alternative without turning it
into an unrequested redesign. Make routine, reversible choices yourself. Ask
the user when meaning, authority, scope or an analyst-facing contract is unclear
or a decision is hard to reverse. When both choices are safe and follow project
rules, let the user decide. Public-data safety and the raw-to-staging boundary
are hard constraints.

Before writing SQL:

- State the subject and grain. Add population and time when the model selects or
  derives them.
- Search model names, YAML and lineage. Start from the most settled useful model
  and move upstream only when its contract is insufficient. Reuse, compose or
  extend where possible; create a model or seed only for a distinct, durable
  contract.
- Only staging may consume raw. Hand-written models use `ref()`.
- Do not guess clinical meaning. Make population, code-list, threshold and date
  rules visible, and ask when their authority or interpretation is unclear.
- Use SQL comments for non-obvious meaning, source quirks or surprising choices,
  not to narrate SQL. Update or remove them when the logic changes.
- Treat reporting and published models as analyst interfaces. Choose grain and
  columns deliberately; remove duplicate, unused or mostly empty fields without
  analytical value. Use clear names and pair opaque codes with authoritative
  labels. A modelling block may remain code-only when the downstream interface
  supplies the labels.

Project configuration is part of the model. Folder placement inherits database,
schema, materialisation, tags and hooks from `dbt_project.yml`; check nearby
models before overriding it and call out changes with wider effects. Tags drive
schedules; hooks apply grants, comments and governance.

Write YAML descriptions as short analyst-facing contracts. State the subject,
what one row represents and the population scope. Explain material inclusion or
exclusion rules, thresholds, dates and definitions without narrating the SQL.
Project hooks publish model descriptions and `persist_docs` publishes column
descriptions to Snowflake metadata used by Snowsight and other tools. Document
units, codes and null meaning where they affect interpretation.

Keep SQL, descriptions, stakeholder ownership and contract tests together. Test
every model's grain with its key or key combination. This project does not use
test-driven development. Beyond grain, add permanent tests only for durable
contracts or errors likely to recur. Tests run on every build and consume
Snowflake compute; do not test implementation details or repeat upstream
assertions.

Check downstream impact with `dbt ls -s model_name+`. Compile and build the
smallest useful selection, then build downstream models whose results may
change. Use `dbt show` only under the data-safety rules below.

Check the branch and worktree status, preserve unrelated work and read the diff
before pushing. Never work on `main`; use a `type/short-description` branch. Use
Conventional Commit form for commits and the pull request title. Open the
description with the problem and why it matters, then give the solution,
validation and review focus. Do not add agent, model or harness attribution.

Focus on what was won or corrected. The scope name is secondary:

- Weak title: `feat: covid flu wide fix`
- Stronger title: `fix(olids): allow empty missing-practice views`
- Weak opening: `add a thin snapshot input containing only source_schema and
  content_date`
- Stronger opening: `The missing-practice views can legitimately return no rows
  when every reference practice is present in OLIDS. Requiring at least one row
  causes valid empty results to fail CI.`

Draft pull requests are fine while work or decisions remain; CodeRabbit reviews
them. Before human review, check against current `main` and surface conflicts.
Do not rebase or force-push a shared branch without the user's approval.

When asked to monitor a pull request, check results posted after the latest
push. Verify automated findings against the diff and source. If asked to resolve
feedback, fix genuine issues and answer inaccurate findings with a brief reason;
do not change code merely to satisfy a bot.

Never include credentials or real patient- or person-level data in repository
files or GitHub text, including seeds, test data, query results, logs, errors,
screenshots and examples. Use synthetic data. High-level aggregates are allowed
when they cannot identify anyone. Do not repeat suspected sensitive values.
Alert repository maintainers because deleting the latest diff does not remove
public history.

Assume agent command output is visible to its provider. Use `dbt show` sparingly
and only for a query designed to return a high-level, non-identifying aggregate;
never use it to preview model rows. If validation needs row-level inspection,
give the user a Snowflake-native query for an approved human-controlled tool and
ask only for the safe aggregate or confirmation needed. Apply the same rule to
ad hoc queries and failing-test SQL.
