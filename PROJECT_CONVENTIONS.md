# Project conventions

This is the repository-wide checklist for model work. The
[dbt onboarding handbook](https://dbt-onboarding.vercel.app/) is the canonical
guide to the reasoning behind these conventions.

## Start with the contract

Before writing SQL:

1. State the subject, population, reference time and grain.
2. Search model names, YAML and lineage for an existing contract.
3. Start with the most settled layer that fits the use: published for its named
   product, reporting for business-ready analysis, then modelling when shared
   meaning is still missing.
4. Decide whether to reuse, compose, extend or create. If creating a model,
   record why nearby models do not fit.

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
| Reference | Hold project-owned lookups and derived reference datasets shared across domains. | Follow neighbouring models |
| Modelling | Give a purposeful transformation or shared domain rule a named, tested home. A model may change grain, but should have one coherent reason to change. | `int_` |
| Reporting | Provide a supported business entity or concept for direct analysis at a documented grain. Marts may be wide when they preserve the core grain and reuse established definitions. | `dim_`, `fct_`, `pit_`, `obt_`, `dq_`, `def_`; programme prefixes where established |
| Published | Serve a named report, dashboard, extract or application. Product selection, audience policy, legal basis and delivery names belong here; shared domain meaning belongs upstream. | Follow the established product family |
| Partner | Serve governed partner datasets and their access mapping. Keep partner policy out of shared reporting models. | `ptr_` for partner-facing marts |
| Semantic | Define Snowflake semantic views over established models. Keep facts, dimensions and metrics aligned with their source contracts. | `sem_` |

Meaning should become more scoped as it moves downstream, not contradict a
promise already made upstream.

Each source object has one canonical staging interface. Reuse or extend that
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
should not create a deep DAG of fragments that have no meaning on their own.

## Programme-specific models

A shared model may own a definition agreed for the whole project. A threshold,
population, period, priority rule, term, label or classification authorised by
one programme has narrower scope. Put that rule or vocabulary in the programme's
folder or schema and compose the shared clinical, person and activity models it
needs; do not change a shared model as though the programme's interpretation
applied to everyone.

Make programme scope visible in both path and name:

- Modelling models keep `int_` and use the established programme subject, such
  as `int_flu_`, `int_smi_`, `int_myria_` or `int_ltc_lcs_`.
- Reporting models keep the standard analytical prefix where the family uses
  one, such as `fct_person_myria_`. A programme may own an established reporting
  prefix such as `cltcs_`.
- Published models make the report, dashboard, extract or application visible
  while following established families such as the Myria `_published` models.
- Search neighbouring programme models before naming a new one. Reuse their
  vocabulary and abbreviation; do not introduce a second prefix for the same
  programme.

The folder alone is not enough when the model name would otherwise look like a
project-wide definition.

## Other scoped and transitional models

Programme scope is one case of a narrower contract. Geography-specific,
legacy-migration and consumer-specific models need the same clarity. If they run
beside a shared model for the same subject and grain, make the narrower scope
visible in the model name, folder/schema and description. Do not give a scoped
variant an unqualified name that could be mistaken for the canonical model.

Before adding a parallel `int_`, `fct_`, `dim_` or `obt_` model, explain why the
existing contract cannot be extended or composed. A transitional model should
also state what it is reconciling with and how consumers can distinguish it.

## SQL and dependencies

- New and changed hand-written dependencies use `ref()`. Only generated raw
  models use `source()`. Existing direct `source()` calls in staging are legacy
  debt; do not copy them. In an enhancement, record them as a non-blocking
  follow-up unless the change adds or depends on the bypass. Never hardcode a
  warehouse relation in model SQL.
- Only staging models may `ref()` a `raw_` model. All other models start from
  staging or a later contract; modelling and reporting may reuse each other's
  models where the dependency is acyclic and the contract fits.
- Staging models read raw models with `ref()` and select columns explicitly.
- Folder placement supplies database, schema, materialisation, tags and hooks
  through `dbt_project.yml`. Check neighbouring models before adding a local
  config override.
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
- All output columns from staging onward must be unquoted `snake_case`, except
  in published models. Published models may use quoted consumer-facing names
  with spaces, case or punctuation and do not need to remove those characters.
  This is optional; unquoted `snake_case` remains valid. Generated raw SQL may
  quote physical input identifiers before assigning project aliases.
- Prefix booleans with `is_` or `has_`; suffix dates with `_date`, timestamps
  with `_at` and identifiers with `_id`.
- Do not carry legacy derived names such as `zcontracttype` or `z_contract_type`
  into new project interfaces. When every consumer should use the derived value,
  give it the unprefixed business name. Use `dv_` when retaining both supplied
  and derived values makes their distinction useful, with the supplied value
  kept as a clearly named column or upstream source field. Existing SLAM and
  community `dv_` families are established interfaces; do not require their
  redesign in an enhancement PR.
- State join cardinality. A right-hand input must be unique at the join key when
  the left-hand grain is meant to survive. Do not use `distinct` to conceal a
  fan-out.
- Keep unknown, false, not applicable and missing evidence distinct where the
  domain distinguishes them.
- Put organisation-wide definitions and terminology in shared models. Keep
  programme, audience and product rules or vocabulary in the folders or schemas
  that own them, with their scope visible in model names.

## Seeds

Use seeds for small, team-owned lookup values, mappings, thresholds and numeric
conversion parameters. Logic must not masquerade as data: seed rows should be
declarative facts or parameters, while executable business rules remain visible
in readable SQL. A seed has become a miniature DSL when it encodes expressions,
branching or precedence whose control flow requires a model or macro to
interpret it. Replace that structure with a clear SQL contract rather than
adding syntax to the seed.

## YAML, documentation and tests

Treat SQL, properties and tests as one change.

- Every model needs a description. Describe its subject, population, reference
  time and grain rather than restating its name.
- New non-raw models need `config.meta.owner.name`.
- Document columns when units, code systems, selection rules, dates, null
  meaning, derivation or relationships affect interpretation.
- New and changed test blocks use `data_tests`. Do not require an enhancement to
  migrate an untouched legacy `tests` block. Test the key or key combination
  that enforces the grain.
- Add accepted-value and relationship tests only when the model contract makes
  those assertions true. Valid nulls or out-of-scope parents should not be
  forced away to satisfy a generic test.
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

A pull request should say:

- why the change exists;
- what each changed model does;
- what was checked, including any downstream limit;
- where reviewer judgement is needed.

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
