# Repository agent guidance

Read `PROJECT_CONVENTIONS.md` before reviewing or changing models. Treat it as
the repository contract for layers, model boundaries, naming, programme scope,
SQL, seeds, testing, public data safety and review scope.

Before implementing, search related models, macros, seeds and documentation.
Prefer extending or composing an existing contract when it represents the same
concept and grain; create a separate pipeline only for a distinct contract.

Check the branch, worktree status and diff, and preserve unrelated work. Do not
work on `main`. Use a `type/short-description` branch and Conventional Commits.
Explain the reason and effect in plain-language commits and pull requests.

This is a public NHS repository. Never include credentials or real patient- or
person-level data in code, seeds, tests, logs, examples, documentation, commits
or pull requests. Use synthetic data or non-identifying aggregates, and do not
repeat suspected sensitive values in comments. Make changes to populations,
clinical definitions, code lists, thresholds or date rules explicit and
validate their effect; never invent clinical meaning.

Run the smallest validation that proves the change, then check affected
downstream models.
