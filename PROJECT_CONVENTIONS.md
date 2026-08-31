# Project conventions

This reference defines the repository-wide rules for model work. The
[dbt onboarding handbook](https://dbt-onboarding.vercel.app/) explains the
reasoning behind them.

In this document, a model contract covers the subject, one-row grain, population,
time scope, columns, meanings, and intended uses. It means dbt's
`contract.enforced` feature only where that feature is named. A model family is
a group of models for the same subject with an established folder or prefix.

## Start with the contract

Before writing SQL:

1. State the subject and grain. Add the population and time basis when the model
   selects or derives them.
2. Search model names, YAML and lineage for an existing contract.
3. Start with the most downstream established contract that fits. Use the
   product's published model for product changes, reporting for direct analysis,
   or modelling when the shared meaning is missing.
4. Reuse, compose or extend where possible. Before creating an overlapping
   model, explain why the existing contract does not fit.

## Public repository data safety

This repository is public. Never commit real patient- or person-level data,
direct identifiers, combinations of values that could identify a person,
row-level extracts, logs or screenshots containing real data. This applies to
seeds, test data, examples, documentation and hardcoded SQL values as well as
model files.

Treat suspected disclosure as a critical blocking finding. Identify the file,
location, and data category without repeating the value. Remove the data and
alert repository maintainers. Deleting it from the latest diff alone may not
remove it from public history. Column names and clearly synthetic examples are
not patient data by themselves.

High-level aggregates such as broad counts, rates, distributions, and validation
totals are not person-level data. They are suitable review evidence only when
their dimensions and cell sizes cannot identify an individual.

## Layer contracts

Raw data has one route into the project: through staging. Only staging models
may reference a `raw_` model. Reference, modelling, reporting, published,
partner, and semantic models must consume staging or a later contract.

Beyond staging, choose a model's area and dependencies from its responsibility,
consumers and required contract, not from whether the SQL contains a join,
filter or window function. This is not a rigid upward ladder: a modelling model
may reuse a reporting model when that is the established interface and does not
create a cycle.

| Area | Contract | Naming |
|------|----------|--------|
| Raw | Generated 1:1 evidence from a source table. Rename physical columns to readable `snake_case`. Do not cast, filter, deduplicate, join, or interpret. Never edit generated raw files by hand. | `raw_` |
| Staging | Provide the single project interface to a source object. Use project names and types and handle universal delivery quirks while normally preserving the source entity, grain and legitimate rows. | `stg_` |
| Reference | Hold project-owned lookups and derived reference datasets shared across domains. | Follow models in the same folder or model family |
| Modelling | Give a purposeful transformation or shared domain rule a named, tested home. A model may change grain, but it must have one coherent responsibility. | `int_` |
| Reporting | Provide a supported business entity or concept for direct analysis at a documented grain. Marts may be wide when they preserve the core grain and reuse established definitions. | `dim_`, `fct_`, `pit_`, `obt_`, `dq_`, `def_`, and established programme prefixes |
| Published | Serve a named report, dashboard, extract, or application. Product selection, audience policy, legal basis, and delivery names belong here. Shared domain meaning belongs upstream. | Follow the established product family |
| Partner | Serve governed partner datasets and their access mapping. Keep partner policy out of shared reporting models. | `ptr_` for partner-facing marts |
| Semantic | Define Snowflake semantic views over established models. Keep facts, dimensions and metrics aligned with their source contracts. | `sem_` |

Downstream models may narrow or tailor a shared definition. Make the narrower
scope explicit in the path, name, and description. Do not silently contradict
the shared definition.

Each source object has one staging interface. Reuse or extend it instead of
adding a staging model for a pipeline, migration, or consumer. Name a new
staging model for the source object and use the established source vocabulary.

Keep staging free of joins unless every consumer needs the join for source
cleaning, standardisation, or enrichment. Put business populations and
consumer-specific rules in a later model. A base model and an established
`_latest` view for a cumulative resubmission feed count as one source interface.

## Model boundaries

Give each model one coherent responsibility. Do not create a model for every
transformation or CTE. Keep steps together when they describe the same concept
and change for the same reason.

Split a model when its parts have independent grains, tests, owners, consumers,
reuse, or reasons to change. Each resulting model needs a meaningful contract,
not a place in a chain of otherwise nameless fragments.

## Programme-specific models

A shared model may own a definition agreed for the whole project. A threshold,
population, period, priority rule, term, label or classification authorised by
one programme has narrower scope. Put that rule or vocabulary in the programme's
folder or schema and reuse the shared clinical, person and activity models it
needs. Do not change a shared model as though the programme's interpretation
applied to everyone.

Make programme scope visible in both path and name:

- Modelling models keep `int_` and use the established programme subject, such
  as `int_flu_`, `int_smi_`, `int_myria_` or `int_ltc_lcs_`.
- Reporting models keep the standard analytical prefix where the family uses
  one, such as `fct_person_myria_`. A programme may own an established reporting
  prefix such as `cltcs_`.
- Published models make the report, dashboard, extract or application visible
  while following established families such as the Myria `_published` models.
- Search programme models in the same folder or model family before naming a
  new one. Reuse their vocabulary and abbreviation. Do not introduce a second
  prefix for the same programme.

The folder alone is not enough when the model name would otherwise look like a
project-wide definition.

## Other scoped and transitional models

Programme scope is one case of a narrower contract. Geography-specific,
legacy-migration and consumer-specific models need the same clarity. If they run
beside a shared model for the same subject and grain, make the narrower scope
visible in the model name and folder/schema. Explain it in the description when
the name is not enough. Do not give a scoped variant an unqualified name that
could be mistaken for the shared model.

When a new model or seed overlaps an existing one, identify the difference in
their contracts. Create a separate object only for a distinct subject, grain,
scope, stakeholder group, or business reason to change. A transitional model
must also state what it reconciles and how consumers can distinguish it.

## SQL and dependencies

- All hand-written models use `ref()`. Only generated raw models use `source()`.
  If a changed hand-written model calls `source()` directly, replace that call
  with `ref()` to the generated raw model. Do not search unrelated files for
  legacy calls. Never hardcode a warehouse relation in model SQL.
- Only staging models may `ref()` a `raw_` model. All other models start from
  staging or a later contract. Modelling and reporting may reuse each other's
  models where the contract fits and does not create a circular dependency.
- Staging models read raw models with `ref()` and select columns explicitly.
- Folder placement supplies database, schema, materialisation, tags and hooks
  through `dbt_project.yml`. Check models in the same folder or model family
  before adding a local config override.
- Use established model vocabulary. Names normally combine a role prefix, a
  domain subject and a suffix only when it explains shape, time or grain, such
  as `_all`, `_latest`, `_current`, `_historical`, `_register` or `_summary`.
- Name a model for its subject and grain, not for the processing step used to
  build it. Terms such as `consolidated`, `enriched`, `processed`, `intermediate`
  and `final` rarely describe a stable contract. Do not require an established
  model rename when an enhancement does not need one.
- Prefer readable SQL. A short local `CASE` is fine. Split or reshape long,
  nested, or repeated `CASE` expressions when they hide the business rule or
  make it hard to test. Add a model, CTE, or lookup only when it clarifies the
  rule.
- Give a shared business definition one canonical home. Do not repeat a
  maintained provider list, code set, threshold, or date rule across models.
  Join to the shared, reference, or programme model that owns maintained data.
  Use a macro for repeated SQL logic and a project variable for a value supplied
  per run or environment. Keep a definition used by one model in that model.
- All output columns from staging onward must be unquoted `snake_case`, except
  in published models. Published models may use quoted consumer-facing names
  with spaces, case, or punctuation and do not need to remove those characters.
  This is optional. Unquoted `snake_case` remains valid. Generated raw SQL may
  quote physical input identifiers before assigning project aliases.
- Do not expose a column known to be null for every row. Map or derive it, or
  omit it until data exists. A model under `models/published/` may use an empty
  placeholder when its documented contract requires a fixed schema. Explain why
  the field is empty. Add branch-specific nulls in the model that performs a
  union, not in its upstream models.
- Use `is_` or `has_` for booleans and make dates, timestamps and identifiers
  clear in the name. Prefer `_date`, `_at` and `_id` where they read naturally,
  but follow an established model-family convention or use another unambiguous
  form such as `date_`.
- Do not carry legacy derived names such as `zcontracttype` or `z_contract_type`
  into new project interfaces. When every consumer should use the derived value,
  give it the unprefixed business name. Use `dv_` when retaining both supplied
  and derived values makes their distinction useful, with the supplied value
  kept as a clearly named column or upstream source field. Existing SLAM and
  community `dv_` families are established interfaces. Do not require their
  redesign in an enhancement pull request.
- Check every join against the stated grain. To retain one row for each
  left-hand record, the right-hand input must have at most one row for each join
  key. Otherwise, aggregate at the join key or choose one record with a
  documented, deterministic rule. Document any intended change in grain. For an
  effective-date lookup, allow at most one reference period to match each row.
  Never use `distinct` to hide join fan-out.
- Before a Snowflake `pivot`, project only the target grain key or keys, pivot
  column and value. Snowflake implicitly groups by every other input column, so
  carrying metadata into the pivot can silently change the result's grain.
- Keep unknown, false, not applicable and missing evidence distinct where the
  domain distinguishes them.
- Analyst-facing reporting and published models should not expose an opaque
  category code or number as the only usable value. Include its authoritative
  label from the source when available. Otherwise, join to the shared reference
  model where the description is maintained. Retain the code alongside the label
  when it supports traceability, stable filtering or joins. A modelling block
  may remain code-only when a downstream reporting or published interface
  supplies the labels. Do not invent labels or repeat maintained mappings in
  local `case` expressions.
- Put project-wide definitions and terminology in shared models. Keep
  programme, audience and product rules or vocabulary in the folders or schemas
  that own them, with their scope visible in model names.
- Publish NHS Data Model and Dictionary code lists from UKHFD in
  `REFERENCE.DATA_DICTIONARY`. Reserve `REFERENCE.TERMINOLOGY` for clinical
  coding systems, maps and code-set definitions such as SNOMED CT, ICD-10,
  OPCS-4, dm+d and BNF.

## Performance

Performance depends on the data processed by each operation, not the length of
the SQL. Fan-out, wide intermediate rows and repeated scans make joins, windows,
grouping and deduplication hash or sort more data. When that work exceeds
warehouse memory, Snowflake spills to disk. Queries then run much longer and can
cost more. Reduce the work before the expensive operation without obscuring the
model contract.

- Keep costly intermediate results no larger than their purpose requires. Where
  it does not change the contract, filter rows and select the required columns
  before large joins, windows, pivots, grouping or deduplication, because each
  operation must otherwise process the extra rows or width. A filter that the
  model always applies should precede the expensive operation when it does not
  depend on that operation's result. Do not move it when doing so would change a
  rank, aggregate, join outcome, or population. Aggregate or select repeated
  child records at the required grain before joining them, and defer descriptive
  enrichment until after that reduction when it does not affect which records
  survive. Snowflake may push a safe predicate down, but do not rely on that
  optimisation when applying the filter early is a clear, safe reduction.
- Avoid repeated scans and sorts of the same large input when one narrow base
  can express the same rules. Each pass can reread and reorder the same data.
  Several `row_number()` or `qualify` passes with the same partition can often
  become one window or intentional aggregation. Keep separate selections when
  they implement different contracts. Reducing a scan is not a reason to make
  the logic harder to understand.
- Use `union all` for large branches known to be disjoint or when cross-branch
  duplicate removal is not part of the contract. Bare `union` must compare the
  combined rows. `distinct` over a wide or post-join result, and distinct or
  ordered list and array aggregates over a large input, also add hashing or
  sorting. A narrow key list or small lookup may use `distinct` plainly. Where
  possible, deduplicate the narrow key and values before building a wide
  aggregate.
- Scrutinise Cartesian or broadly matching rule-table joins followed by grouping
  or deduplication, and repeated correlated lookups against a large input. They
  can create or revisit many candidate rows only to discard most of them later.
  Join only candidate rows or compute the selection once when the contract
  allows it.
- Base a performance finding on an operation in the changed SQL that plausibly
  creates a large intermediate result, using any scale information already in
  the pull request. A wide output, pivot, window, or several small reference
  joins is not a defect by itself. When size or impact is unknown, state the
  assumption and keep the comment non-blocking. You may suggest Query Profile,
  spill, and pruning checks as optional validation. Do not require them for the
  review.
- Consider `cluster_by` for a materialised model when several downstream
  consumers repeatedly filter or join a large result on the same selective
  columns. The current build pays to sort its output so later queries can avoid
  scanning irrelevant micro-partitions. It does not make the current model's
  upstream reads cheaper. OLIDS clinical event inputs are usually already
  clustered by mapped concept code for the expensive code-filter scan. After
  selecting those rows, a reused result may instead cluster by person for its
  downstream joins. Follow an established model-family key without requiring
  fresh proof. For a new or unusual key, explain the visible downstream access
  pattern and the build-cost trade-off. Optional before-and-after measurement can
  confirm the benefit, but is not a review requirement. See the
  [onboarding clustering page](https://dbt-onboarding.vercel.app/advanced/clustering).

## Seeds

Use seeds for small, team-owned lookup values, mappings, thresholds, priorities
and conversion parameters. Logic must not masquerade as data. A seed should not
store expressions, operators or branching that a model or macro must parse.
Keep those rules visible in readable SQL rather than adding syntax to the seed.

## YAML, documentation and tests

Treat SQL, model YAML and contract tests as one change. This project does not
use test-driven development. Every model must test its stated grain with its key
or key combination. Beyond grain, add a permanent test only when it protects a
durable contract or an error likely to recur. Tests run on every build and
consume Snowflake compute, so do not test implementation details or repeat
assertions already owned upstream. The Model Test Coverage CI check verifies
that a changed model has a test. It cannot infer the model's grain. The test
itself runs in local builds and the merge-queue development build. Authors and
reviewers must still confirm that it covers the stated key or key combination.

- Every model needs a description of its subject and what one row represents.
  State the time basis where it matters. When a model filters or derives a
  population, explain its inclusion and exclusion rules, including thresholds
  and date rules, so an analyst can tell who or what is selected. Name the code
  list, value set or upstream definition used instead of copying a long list of
  codes into prose. Do not invent population text for a staging or lookup model
  that does no selection.
- Write descriptions for analysts. Project hooks publish model descriptions as
  Snowflake object comments and `persist_docs` publishes column descriptions,
  making them visible in Snowsight and other tools that read warehouse metadata.
  Describe business meaning and interpretation rather than narrating the SQL.
- New non-raw models need `config.meta.owner.name`. The owner is the business
  stakeholder or group accountable for the model's meaning and use, not the
  person who happened to make the code change.
- Document columns when units, code systems, dates, null
  meaning, derivation or relationships affect interpretation.
- New and changed test blocks use `data_tests`. Do not require an enhancement to
  migrate an untouched legacy `tests` block.
- Focus tests on assumptions most likely to break when sources or rules change:
  grain, join uniqueness, population boundaries and membership of maintained
  lists. Prefer a relationship test to the model where a changing list is
  maintained over copying its values into an accepted-values test. Add
  accepted-value, not-null and relationship tests only when the contract makes
  them true. Valid nulls or out-of-scope parents should not be forced away to
  satisfy a generic test.
- Put a test where its promise is made. Downstream models should test their new
  composition and grain, not copy every upstream assertion.
- Use singular tests for domain rules that generic tests cannot express. Select
  only the model keys and context needed to diagnose a failure. Treat any
  returned rows as potentially patient-level under the safety rules above.

## Validate the change

Build the smallest selection that answers the current question, then widen it
when the change can affect consumers:

```powershell
dbt compile -s my_model
dbt build -s my_model
dbt ls -s my_model+
dbt build -s my_model+
```

A pull request should normally merge only after it demonstrates correct
behaviour, a clear model contract and performance suitable for the expected
scale. This does not require speculative tuning or a benchmark for an ordinary
change.

Assume output from commands run by a coding agent is visible to its provider.
`dbt show` executes SQL and returns its result. Use it sparingly in an agent
session, and only when the query is designed to return a high-level,
non-identifying aggregate. Do not use it to preview model rows. When validation
needs row-level inspection, give the user a ready-to-run Snowflake-native query
for a human-controlled Snowflake session approved for patient-level data. Ask
only for the non-identifying aggregate or confirmation needed. Apply the same
rule to ad hoc queries and failing-test SQL.

Use `.\build_changed.ps1` to build models changed on the branch. Add `-d` when
downstream models may be affected and `-u` when their parent models must also be
rebuilt.

For logic changes, compare development and production with aggregate or
non-identifying checks. Never put credentials, identifier values, row-level
outputs or screenshots of real data in this public repository or its pull
requests.

A pull request needs enough context to explain why the change exists. One clear
sentence can be enough for a small change. Add changed behaviour, checks,
downstream limits or questions needing reviewer judgement when they help someone
assess the change.

## Review scope

Review the behaviour introduced or changed and its effect on the model's
existing contract. An enhancement does not make its author responsible for a
sweeping redesign of code that was already there.

First identify the change type:

- For a new model, review its responsibility, boundary, scope and name.
- For an enhancement, review the addition and its effect on the existing
  contract. Do not imply a redesign of inherited code.
- For an explicitly transitional legacy migration, enforce raw/staging
  boundaries, grain and safety. When matching the legacy output is the stated
  goal, record inherited internal design as a non-blocking follow-up rather than
  rebuilding it in the migration pull request.

Pre-existing design debt should block the pull request only when the change
worsens it, depends on it, or cannot be merged safely without resolving it.
Reviewers may still record a concern visible in the changed area when it will
help future work. Label it `follow-up (non-blocking):`, keep it out of the merge
recommendation and do not search for unrelated debt. If an addition gives the
model a second responsibility, review the boundary needed for that addition
rather than requiring the contributor to rebuild the whole model.

When SQL, descriptions and tests disagree about business behaviour, flag the
contradiction and ask which contract is authoritative. Do not prescribe a
population-changing fix unless the intended rule is evidenced in the pull
request or an authoritative source.
