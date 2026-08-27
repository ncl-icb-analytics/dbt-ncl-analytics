# Repository agent guidance

Read `PROJECT_CONVENTIONS.md` before reviewing or changing models. Treat it as
the repository contract for layers, model boundaries, naming, programme scope,
SQL, seeds, testing, public data safety and review scope.

Before editing, check the current branch, worktree status and existing diff.
Preserve unrelated work. Do not work directly on `main`; use a focused
`type/short-description` branch such as `docs/coderabbit-project-guidance`.

Keep changes proportional to the request. Do not make contributors redesign
unrelated inherited code. Record useful existing debt as a non-blocking
follow-up unless the change worsens it, depends on it or cannot be safe without
resolving it.

Use Conventional Commits, for example
`docs(coderabbit): align project review guidance`.

Write commit messages and pull-request descriptions in plain language. Avoid
unexplained jargon, abbreviations and internal shorthand. Explain why the
change is needed, the decision it implements and its expected effect; do not
merely list changed files or restate the diff. Keep a Conventional Commit
subject brief, and use the commit body when the rationale is not clear from the
subject. A pull request should make sense without access to the conversation
that led to it.

Run the smallest validation that proves the change, then check affected
downstream models. Never put credentials or real patient- or person-level data
in this public repository, its commits or pull requests.
