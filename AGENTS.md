# Working in dbt-analytics

This is a public NHS dbt project on Snowflake. Read
`PROJECT_CONVENTIONS.md` before model work; the
[dbt onboarding handbook](https://dbt-onboarding.vercel.app/) explains the
reasoning behind it.

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

Keep SQL, descriptions, stakeholder ownership and tests for one contract change
together. Check downstream impact with `dbt ls -s model_name+`. Validate the
smallest useful selection with `dbt compile`, `dbt show` and `dbt build`, then
build affected downstream models when the contract can change their results.

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
