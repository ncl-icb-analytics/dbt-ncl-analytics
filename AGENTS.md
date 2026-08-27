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

Remember that project configuration is part of the model:

- Folder placement supplies the database, schema, materialisation, tags and
  post-hooks through `dbt_project.yml`. Tags drive build schedules and hooks
  apply grants, comments and governance. Check nearby models and inherited
  config before adding an override; call out `dbt_project.yml` changes because
  they can affect many models.
- Write YAML descriptions for analysts, not as implementation notes. Project
  hooks publish model descriptions as Snowflake object comments, while
  `persist_docs` publishes column descriptions. They appear in Snowsight and
  other tools that read warehouse metadata. Describe grain, population, time,
  units, codes and null meaning where they affect interpretation.

Keep SQL, descriptions, stakeholder ownership and tests for one contract change
together. Check downstream impact with `dbt ls -s model_name+`. Validate the
smallest useful selection with `dbt compile`, `dbt build` and, where its output
is safe to expose, `dbt show`. Then build affected downstream models when the
contract can change their results.

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

Assume command output is visible to the agent provider. `dbt show` executes the
model query and returns rows: use it in an agent session only for synthetic or
non-identifying output, or when the tool has approved zero-data-retention
controls for that data. Apply the same rule to ad hoc queries and failing-test
SQL. Otherwise prefer builds, tests and non-identifying aggregate checks, and
inspect row-level data in an approved human-controlled tool.
