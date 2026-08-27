# Working in dbt-analytics

This is a public NHS dbt project on Snowflake. Read
`PROJECT_CONVENTIONS.md` before model work; the
[dbt onboarding handbook](https://dbt-onboarding.vercel.app/) explains the
reasoning behind it.

Look carefully before changing anything. Understand the intended outcome, the
real constraint and the relevant surrounding code, configuration and lineage.
Then make the smallest coherent change that makes the correct behaviour easy to
understand. Solve the need in front of you; do not add flexibility, models,
macros or configuration for hypothetical future requirements. Reuse settled
code where it fits, but do not preserve complexity merely because it already
exists. Keep the work focused; simplify nearby code only when that is needed to
make the requested change clear and safe.

If the proposed direction is likely to produce wrong results, an unclear
contract, a duplicate pipeline or avoidable cost and maintenance, say so before
implementing it. Explain the concrete consequence and offer the smallest
realistic alternative. Do not turn design feedback into an unrequested
redesign. When both approaches are safe and compatible with project rules, let
the user decide and follow that decision.

Use these instructions as project defaults, not as a substitute for judgement.
The requested outcome and the model's established contract should guide the
implementation. If a default does not fit, explain the trade-off rather than
applying it mechanically. Public-data safety and the raw-to-staging boundary
remain hard constraints. Make routine, reversible implementation choices
yourself. Defer to the user when the intended outcome or clinical or business
meaning is unclear, when the authority for a definition is uncertain, or when a
decision would materially widen the scope, change an analyst-facing contract or
be hard to reverse.

Before writing SQL:

- State the subject and grain. Add population and time when the model selects or
  derives them.
- Search model names, YAML and lineage. Start from the most settled useful model
  and move upstream only when the required contract is missing.
- Reuse, compose or extend an existing contract where it fits. Create a model or
  seed only for a distinct, durable contract; do not invent a parallel pipeline.
- Only staging may consume raw. Hand-written models use `ref()`.
- Do not guess clinical meaning. Make population, code-list, threshold and date
  rules visible, and ask when their authority or interpretation is unclear.
- Use SQL comments to explain non-obvious business meaning, source quirks or why
  a surprising choice is needed. Do not narrate obvious SQL. Update or remove a
  comment when its logic changes.
- Make supported reporting and published models understandable without extra
  analyst lookups. Pair categorical codes with authoritative labels from the
  source or a shared reference model. A modelling block may remain code-only
  when the downstream analyst-facing interface supplies the labels.

The aim of reporting is to make each domain easy for analysts to understand and
use. Choose the grain and output columns deliberately. Keep fields that explain
the subject or support analysis; remove duplicate, unused or mostly empty fields
when they have no clear analytical value. Do not carry columns forward merely
because they exist upstream. Use names and descriptions that let an analyst
interpret the model without reading its SQL.

Remember that project configuration is part of the model:

- Folder placement supplies the database, schema, materialisation, tags and
  post-hooks through `dbt_project.yml`. Tags drive build schedules and hooks
  apply grants, comments and governance. Check nearby models and inherited
  config before adding an override; call out `dbt_project.yml` changes because
  they can affect many models.
- Write YAML descriptions as short analyst-facing contracts, not implementation
  notes. State the subject, what one row represents and the population scope. If
  the model introduces complex inclusion or exclusion rules, explain them
  clearly enough for an analyst to understand who or what is selected, including
  material thresholds, dates and the definition used. Do not write an essay or
  narrate the SQL. Project hooks publish model descriptions as Snowflake object
  comments, while `persist_docs` publishes column descriptions. They appear in
  Snowsight and other tools that read warehouse metadata. Document units, codes
  and null meaning where they affect interpretation.

Keep SQL, descriptions, stakeholder ownership and contract tests together. Test
every model's stated grain with its key or key combination. This project does
not use test-driven development. Beyond grain, add a permanent test only when it
protects a durable contract or an error likely to recur. Tests run on every
build and consume Snowflake compute, so do not test implementation details or
repeat assertions already owned upstream.

Check downstream impact with `dbt ls -s model_name+`. Validate the smallest
useful selection with `dbt compile` and `dbt build`, then build affected
downstream models when the contract can change their results. Use `dbt show`
only under the data-safety rules below.

Check the branch, worktree status and diff, and preserve unrelated work. Never
work on `main`; use a `type/short-description` branch and Conventional Commits.
Before pushing, read the diff. In the pull request, explain why the change is
needed, what contract changed, what you checked and where review is needed.

Never include credentials or real patient- or person-level data in repository
files or GitHub text. This includes seeds, test data, row-level query results,
logs, error output, screenshots and examples containing real data. Use synthetic
data; high-level aggregates are valid evidence when they cannot identify anyone.
Do not repeat suspected sensitive values. Alert repository maintainers because
deleting the latest diff does not remove public history.

Assume command output is visible to the agent provider. `dbt show` executes SQL
and returns its result. Use it sparingly, and only when the query is designed to
return a high-level, non-identifying aggregate. Do not use it to preview model
rows. If validation needs row-level inspection, give the user a ready-to-run
Snowflake-native query for an approved human-controlled tool, and ask only for
the non-identifying aggregate or confirmation needed; do not ask them to paste
row-level results into the agent session. Apply the same rule to ad hoc queries
and failing-test SQL. Otherwise prefer builds, tests and non-identifying
aggregate checks.
