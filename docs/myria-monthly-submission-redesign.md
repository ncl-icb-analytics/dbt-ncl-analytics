# Proposal: Myria monthly submission as a "download-and-send" table

**Status:** Draft for comment (not ready to merge). Needs evaluation-owner sign-off
(Kate / Jess / RFL) before any code lands, because it changes the evaluation cohort.

## Why

Today the monthly RFL submission and the propensity-match evaluation involve hand-steps:
editing dated `UNION` blocks as new months arrive, uploading files, re-running by hand.
That "fiddling" is the source of the handover anxiety. The goal here is to make it **a
process you run, not something you maintain**: a dbt-built table that *is* the monthly
extract — download it and send it, no code.

This builds on the canonical inputs landed in #788 (the `DATA_LAKE__NCL.MYRIA` schema,
loader, and `raw_myria_*` / `stg_myria_*` models).

## Two views, both wanted

- **As-received / point-in-time** — each month's file is a snapshot of who was in scope
  when we produced it. You report a month using *that month's* extract.
- **Over-time window** — the longitudinal view (the SCD2 history, the pre/post evaluation
  windows). A different question, served by the continuous history.

## Proposed shape

1. **`reporting_month` as a first-class primitive.** Group files by month; the
   representative for a month is the latest file in it (handles >1 file in a month).
   Cheap, data-driven, and already true for the file-based streams (enrolled, matched).

2. **A monthly-submission table = the extract to send.** `reporting_month`-stamped, the
   exact rows that go to RFL. Download and send — no code.

3. **Record the send instead of inferring it.** Snapshot/append that extract at send time,
   so the history *is* the exact monthly submissions (`reporting_month` + the cohort
   actually sent). The evaluation then reads that history — **no seed, no hardcoded dates,
   no guessing which day was the send**.

This is the same pattern as the MYRIA inputs: one stamped append, flowed downstream.

## Why this isn't a drop-in (the bit needing sign-off)

The current eligible cohort reconstructs "what was sent" by reading the snapshot *as of*
five hardcoded send dates. Any data-driven replacement has to anchor on a consistent point
(e.g. month-end), because the actual send dates (≈2nd Tuesday) are **not recorded anywhere**.
Measured divergence of a month-end approach vs the current prod cohort:

- **+166 / −37** patients
- **~4,750 of 6,061** common patients land in a **different `reporting_month`**

Most of that is because the prod hardcoded version effectively **drops January** (a
timestamp-vs-date boundary: `date('2026-01-22')` at midnight falls before that day's first
snapshot timestamp, so the `between` returns nothing — the old "no results for January"
note), pushing first-appearances to February. A data-driven version captures January, so
nearly everyone's anchor shifts. The data-driven version is arguably *more* correct, but it
**changes the evaluation's pre/post reference points** — so it is a methodology decision,
not a refactor.

## Validation note (important for reviewing this)

The dbt **snapshot is the source of the cohort**, and the dev and prod snapshots are
**separate stateful lineages** (dev had 14 version-dates, prod 63). A raw dev-table-vs-
prod-table diff therefore mismatches on every snapshot-sourced column for environment
reasons, not code. **Validate cohort changes by running the logic against the prod
snapshot** (or in prod) — not by diffing dev against prod tables.

## Open questions for the evaluation owners

1. Is a consistent monthly checkpoint (e.g. month-end) acceptable as the cohort anchor, vs
   the exact send date? (It shifts pre/post windows — the ~4,750 figure above.)
2. Should January be included now that a data-driven version captures it?
3. Do we persist each monthly submission as a recorded artifact (so history = exact sends)?
4. Confirm the monthly-submission table is the single "download-and-send" source for RFL.
