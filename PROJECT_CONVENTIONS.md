# Project conventions

This is the repository-wide checklist for model work. The
[dbt onboarding handbook](https://dbt-onboarding.vercel.app/) is the main guide
to the reasoning behind these conventions.

## Start with the contract

Before writing SQL:

1. State the subject and grain. Add the population and time basis when the model
   selects or derives them.
2. Search model names, YAML and lineage for an existing contract.
3. Start with the most settled layer that fits the use: published for its named
   product, reporting for business-ready analysis, then modelling when shared
   meaning is still missing.
4. Decide whether to reuse, compose, extend or create. When an existing model
   overlaps the proposed contract, record why it does not fit.

## Public repository data safety

This repository is public. Never commit real patient- or person-level data,
direct identifiers, combinations of values that could identify a person,
row-level extracts, logs or screenshots containing real data. This applies to
seeds, test data, examples, documentation and hardcoded SQL values as well as
model files.

Treat suspected disclosure as a critical blocking finding. Identify the file,
location and data category without repeating the value. Remove the data and
alert repository maintainers; deleting it from the latest diff alone may not
remove it from public history. Column names and clearly synthetic examples are
not patient data by themselves.

High-level aggregates such as broad counts, rates, distributions and validation
totals are not person-level data merely because they were calculated from
patient data. They are suitable review evidence when their dimensions and cell
sizes cannot identify an individual.

## Layer contracts

Raw has one allowed route: through staging. Only staging models may reference a
`raw_` model. Models in reference, modelling, reporting, published, partner and
semantic areas must consume staging or a later contract.

Beyond staging, choose a model's area and dependencies from its responsibility,
consumers and required contract, not from whether the SQL contains a join,
filter or window function. This is not a rigid upward ladder: a modelling model
may reuse a reporting model when that is the established interface and does not
create a cycle.

| Area | Contract | Naming |
|------|----------|--------|
| Raw | Generated 1:1 evidence from a source table. Rename physical columns to readable `snake_case`; do not cast, filter, deduplicate, join or interpret. Never edit generated raw files by hand. | `raw_` |
| Staging | Provide the single project interface to a source object. Use project names and types and handle universal delivery quirks while normally preserving the source entity, grain and legitimate rows. | `stg_` |
| Reference | Hold project-owned lookups and derived reference datasets shared across domains. | Follow models in the same folder or model family |
| Modelling | Give a purposeful transformation or shared domain rule a named, tested home. A model may change grain, but should have one coherent reason to change. | `int_` |
| Reporting | Provide a supported business entity or concept for direct analysis at a documented grain. Marts may be wide when they preserve the core grain and reuse established definitions. | `dim_`, `fct_`, `pit_`, `obt_`, `dq_`, `def_`; programme prefixes where established |
| Published | Serve a named report, dashboard, extract or application. Product selection, audience policy, legal basis and delivery names belong here; shared domain meaning belongs upstream. | Follow the established product family |
| Partner | Serve governed partner datasets and their access mapping. Keep partner policy out of shared reporting models. | `ptr_` for partner-facing marts |
| Semantic | Define Snowflake semantic views over established models. Keep facts, dimensions and metrics aligned with their source contracts. | `sem_` |

Downstream models may narrow or tailor a shared definition, but must not silently
contradict it.

Each source object has one staging interface. Reuse or extend that
model instead of adding another staging model for a pipeline, migration or
consumer. Name new staging models for the source object they clean, using the
project's established source and entity vocabulary.

Staging should normally have no joins. A join is justified only when its sole
purpose is universal source cleaning, standardisation or enrichment that every
consumer should inherit. Business populations and consumer-specific rules
belong in a later model. A base model plus an established `_latest` view for a
cumulative resubmission feed is one source interface, not a duplicate.

## Model boundaries

Use one model for one coherent responsibility, not one model for each
transformation or CTE. Several steps belong together when they describe the
same concept and normally change for the same reason.

Split a model when parts have independent grains, tests, owners, consumers,
reuse or reasons to change. A split should give each contract a clear home; it
should not create a deep dependency chain of fragments that have no meaning on
their own.

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
  new one. Reuse their vocabulary and abbreviation; do not introduce a second
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

When a new model or seed appears to duplicate an existing one, ask how its
contract differs and whether the existing model or seed can be extended,
composed or reused. A separate object should have a distinct subject, grain,
scope, stakeholder group or business reason to change. A transitional model
should also state what it is reconciling with and how consumers can distinguish
it.

## SQL and dependencies

- All hand-written models use `ref()`. Only generated raw models use `source()`.
  If a changed hand-written model already calls `source()` directly, replace the
  call with `ref()` to the generated raw model as part of the change. This small
  fix is not a non-blocking follow-up. Do not search unrelated files for legacy
  calls. Never hardcode a warehouse relation in model SQL.
- Only staging models may `ref()` a `raw_` model. All other models start from
  staging or a later contract; modelling and reporting may reuse each other's
  models where the contract fits and this does not create a circular dependency.
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
  model to be renamed merely because an enhancement touches it.
- Prefer readable, straightforward SQL. A short local `CASE` is fine. Split or
  reshape long, nested or repeated `CASE` expressions when their branching hides
  the business rule or makes it hard to test. Use a named model, CTE or lookup
  only when it makes the rule clearer.
- Give a business definition one owner when more than one model depends on it.
  Do not repeat the same maintained provider list, code set, threshold or date
  rule across model SQL. Join to the shared, reference or programme-scoped model
  that owns maintained data; use a macro for reused SQL logic and a project
  variable only for a value intended to be supplied per run or environment. A
  value that defines one model's concept belongs in that model even if it may
  change. Do not create another DAG node merely to extract a local literal.
- All output columns from staging onward must be unquoted `snake_case`, except
  in published models. Published models may use quoted consumer-facing names
  with spaces, case or punctuation and do not need to remove those characters.
  This is optional; unquoted `snake_case` remains valid. Generated raw SQL may
  quote physical input identifiers before assigning project aliases.
- Do not add an output column implemented as a literal `null` or otherwise known
  to be null for every row. It advertises data that the model does not provide
  and leaves consumers unable to distinguish an unpopulated field from missing
  data. Map or derive the column, or omit it until it can be populated. A
  placeholder is allowed only in a model under `models/published/`, when its
  documented output contract requires a fixed schema. Describe the contract and
  why the field is empty. Where union branches need compatible shapes, add
  branch-specific nulls in the model performing the union rather than exposing
  all-null columns from upstream models.
- Use `is_` or `has_` for booleans and make dates, timestamps and identifiers
  clear in the name. Prefer `_date`, `_at` and `_id` where they read naturally,
  but follow an established model-family convention or use another unambiguous
  form such as `date_`.
- Do not carry legacy derived names such as `zcontracttype` or `z_contract_type`
  into new project interfaces. When every consumer should use the derived value,
  give it the unprefixed business name. Use `dv_` when retaining both supplied
  and derived values makes their distinction useful, with the supplied value
  kept as a clearly named column or upstream source field. Existing SLAM and
  community `dv_` families are established interfaces; do not require their
  redesign in an enhancement PR.
- Check joins against the model's stated grain. If the result should keep one
  row for each left-hand record, the right-hand input must have at most one row
  for each join key; otherwise rows and counts are multiplied. Aggregate several
  contributing rows with an intentional `group by` at the join key, or select
  one record with a documented, deterministic rule. Document the resulting grain
  when a join is intended to change it. For an effective-date lookup, ensure no
  more than one reference period can match each left-hand row. Do not add
  `distinct` after a join to hide duplicated rows: it can discard real
  differences and makes the warehouse deduplicate the already multiplied result,
  increasing memory use and spill.
- Before a Snowflake `pivot`, project only the target grain key or keys, pivot
  column and value. Snowflake implicitly groups by every other input column, so
  carrying metadata into the pivot can silently change the result's grain.
- Keep unknown, false, not applicable and missing evidence distinct where the
  domain distinguishes them.
- Put organisation-wide definitions and terminology in shared models. Keep
  programme, audience and product rules or vocabulary in the folders or schemas
  that own them, with their scope visible in model names.

## Performance

Performance depends on the data processed by each operation, not the length of
the SQL. Fan-out, wide intermediate rows and repeated scans make joins, windows,
grouping and deduplication hash or sort more data. When that work exceeds
warehouse memory, Snowflake spills to disk; queries then run much longer and can
cost more. Reduce the work before the expensive operation without obscuring the
model contract.

- Keep costly intermediate results no larger than their purpose requires. Where
  it does not change the contract, filter rows and select the required columns
  before large joins, windows, pivots, grouping or deduplication, because each
  operation must otherwise process the extra rows or width. A filter that the
  model always applies should precede the expensive operation when it does not
  depend on that operation's result; do not move it when doing so would change a
  rank, aggregate, join outcome or population. Aggregate or select repeated
  child records at the required grain before joining them, and defer descriptive
  enrichment until after that reduction when it does not affect which records
  survive. Snowflake may push a safe predicate down, but do not rely on that
  optimisation when applying the filter early is a clear, safe reduction.
- Avoid repeated scans and sorts of the same large input when one narrow base
  can express the same rules. Each pass can reread and reorder the same data.
  Several `row_number()` or `qualify` passes with the same partition can often
  become one window or intentional aggregation. Keep separate selections when
  they implement different contracts; reducing a scan is not a reason to make
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
  assumption and keep the comment non-blocking. Query Profile, spill and pruning
  checks may be suggested as optional validation; do not require the contributor
  to provide them for the review.
- Consider `cluster_by` for a materialised model when several downstream
  consumers repeatedly filter or join a large result on the same selective
  columns. The current build pays to sort its output so later queries can avoid
  scanning irrelevant micro-partitions; it does not make the current model's
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

Treat SQL, properties and tests as one change.

- Every model needs a description of its subject and what one row represents.
  State the time basis where it matters. When a model filters or derives a
  population, explain its inclusion and exclusion rules, including thresholds
  and date rules, so an analyst can tell who or what is selected. Name the code
  list, value set or upstream definition used instead of copying a long list of
  codes into prose. Do not invent population text for a staging or lookup model
  that does no selection.
- New non-raw models need `config.meta.owner.name`.
- Document columns when units, code systems, dates, null
  meaning, derivation or relationships affect interpretation.
- New and changed test blocks use `data_tests`. Do not require an enhancement to
  migrate an untouched legacy `tests` block. Test the key or key combination
  that enforces the grain.
- Focus tests on assumptions most likely to break when sources or rules change:
  grain, join uniqueness, population boundaries and membership of maintained
  lists. Prefer a relationship test to the model that owns a changing list over
  copying its values into an accepted-values test. Add accepted-value, not-null
  and relationship tests only when the contract makes them true. Valid nulls or
  out-of-scope parents should not be forced away to satisfy a generic test.
- Put a test where its promise is made. Downstream models should test their new
  composition and grain, not copy every upstream assertion.
- Use singular tests for domain rules that generic tests cannot express. Return
  columns that identify and explain failing rows.

## Validate the change

Build the smallest selection that answers the current question, then widen it
when the change can affect consumers:

```powershell
dbt compile -s my_model
dbt show -s my_model --limit 20
dbt build -s my_model
dbt ls -s my_model+
dbt build -s my_model+
```

Use `.\build_changed.ps1` to build models changed on the branch. Add `-d` when
downstream models may be affected and `-u` when changed models need rebuilt
parents.

For logic changes, compare development and production with aggregate or
non-identifying checks. Never put credentials, identifiers, row-level outputs or
screenshots of real data in this public repository or its pull requests.

A pull request needs enough context to explain why the change exists. One clear
sentence can be enough for a small change. Add changed behaviour, checks,
downstream limits or points for reviewer judgement when they help someone assess
the change.

## Review scope

Review the behaviour introduced or changed and its effect on the model's
existing contract. An enhancement does not make its author responsible for a
sweeping redesign of code that was already there.

First identify the change type:

- A new model puts its responsibility, boundary, scope and name in review.
- An enhancement puts the addition and its effect on the existing contract in
  review; redesign of inherited code is not implied.
- An explicitly transitional legacy migration must still obey raw/staging
  boundaries, preserve its stated grain and avoid unsafe behaviour. When faithful
  parity is the stated goal, inherited internal design can be recorded as a
  non-blocking follow-up rather than rebuilt in the migration pull request.

Pre-existing design debt should block the pull request only when the change
worsens it, depends on it, or cannot be merged safely without resolving it.
Reviewers may still mention a concern visible in the reviewed area when recording
it will help future work. Label it `follow-up (non-blocking):`, keep it out of the
merge recommendation and do not expand the review into a search for unrelated
debt. If an addition gives the model a second responsibility, review the new
boundary needed for that addition rather than requiring the contributor to
rebuild the whole model.

When SQL, descriptions and tests disagree about business behaviour, flag the
contradiction and ask which contract is authoritative. Do not prescribe a
population-changing fix unless the intended rule is evidenced in the pull
request or an authoritative source.
