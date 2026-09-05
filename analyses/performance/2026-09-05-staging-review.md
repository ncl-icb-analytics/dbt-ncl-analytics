# Staging and shared model performance

The SUS organisation lookups are a measured improvement with equivalent
outputs. Keep the current staging views and schedules. The clustering and
diagnosis materialisation trials below need stronger savings before adoption.

## SUS organisation lookups

The previous `clean_organisation_id` expression used an `IN` subquery. Its
outpatient plan grouped the wide source rows and spilled to local storage.
Joining a distinct set of organisation dictionary codes avoids that operation.
Both versions retain the full source code for dictionary type 41, otherwise
take its first three characters, and preserve nulls.

| Staging model | Source rows | Original execution | Revised execution | Local spill |
| --- | ---: | ---: | ---: | ---: |
| `stg_sus_ecds_emergency_care` | 18,021,260 | 17.65s | 7.17s | None in either run |
| `stg_sus_op_appointment` | 112,298,814 | 27.87s | 16.32s | 1.96 GiB to zero |

These are full-output temporary table builds on `WH_WNL_ENGINEERING_M` with
result caching disabled. Timing includes execution, excludes queueing and does
not remove warehouse cache effects. They are paired observations, not a runtime
guarantee. A full-column multiset comparison found zero differing row groups
for both models, including duplicate multiplicity.

`int_sus_uec_encounter` now reads the already cleaned staging provider code.
Applying the same cleanup twice does not change the value. The organisation
dictionary joins, other fields and population rules remain unchanged.

Benchmark query IDs:

- Emergency original: `01c6daed-0001-f006-0001-51c216210e0e`.
- Emergency revised: `01c6daed-0001-f006-0001-51c216210ed2`.
- Outpatient original: `01c6daee-0001-f005-0001-51c216211ef2`.
- Outpatient revised: `01c6daee-0001-f006-0001-51c2162141b6`.

The development build selected both staging models and their direct consumers:
eight models built and 30 tests passed. Seven data tests failed. One reports
1,198,788 unresolved independent-provider names; six report relationships from
existing emergency child tables to the rebuilt encounter table.

Rebuilding the original emergency SQL against the same development inputs and
comparing every output column found zero differing row groups across 18,021,260
rows. The original also produced the same 1,198,788 unresolved names. Since the
parent output is identical and the child tables were unchanged, the six
relationship failures are also unaffected by this change.
The exact emergency comparison query was
`01c6dafa-0001-efff-0001-51c2162194aa`.

A subsequent targeted development build refreshed all 95 upstream resources
for those seven checks. All 95 resources built successfully and all seven checks
then passed. No test definitions or business rules changed to clear them.

## Repeated staging view work

Direct model consumers come from the dbt manifest. Counts exclude data tests
and do not represent the number of physical scans.

- `stg_olids_observation` has 268 direct consumers and
  `stg_olids_medication_order` has 70. Both are projections with deletion and
  person filters. Their upstream OLIDS tables already have clustering keys:
  observation uses `(mapped_concept_code, clinical_effective_date)` and
  medication uses `(bnf_chapter, mapped_concept_code, clinical_effective_date)`.
  Queries through the views can use that physical organisation.
- `stg_sus_ecds_clinical_diagnoses_snomed` repeats a deduplication window for
  three consumers. Materialising it took 3.16s and saved 4.24s across those
  consumers in the trial. That margin is too small to justify a freshness change.
- `stg_sus_op_appointment_clinical_coding_diagnosis_icd` has two consumers.
  Materialising it took 3.03s and saved 2.14s across their queries.
- `stg_reference_combined_codesets` and
  `stg_reference_ukhsa_codecluster_versions` aggregate their source data and have
  140 and 38 consumers. Their physical inputs are only about 22 MiB and 3 MiB.
  Consumers filter by cluster and source or version. High reuse alone does not
  justify copying these small lookups into new tables.
- `stg_mhsds_activesubmission` repeats a header-selection window across 17
  consumers. This remains a profiling candidate; its contribution to consumer
  runtime was not isolated in this review.

The diagnosis view trials compared full consumer queries wrapped in row count
and full-row aggregate hash checks. All five consumer comparisons matched.
These hash checks are probabilistic and differ from the exact staging checks.

## Large shared outputs without explicit clustering

| Model | Rows | Stored size | Direct consumers |
| --- | ---: | ---: | ---: |
| `int_observations` | 367,278,015 | 12.06 GiB | 4 |
| `int_sus_op_appointment` | 112,078,412 | 5.22 GiB | 11 |
| `int_sus_op_encounter` | 80,829,169 | 3.78 GiB | 4 |
| `int_sus_uec_encounter` | 18,010,094 | 2.01 GiB | 12 |

Storage metadata is a snapshot and can lag builds. The staging source counts
above can therefore exceed the stored downstream counts.

Cancer, dialysis and maternity consumers join outpatient appointments by
`visit_occurrence_id`. Sorting a temporary copy on that key still scanned all
253 partitions for cancer and maternity, and 241 for dialysis. Combined
execution improved by only 1.16s across the three queries. Do not add this key
just because many consumers join on it.

The same consumers filter `int_observations` by vocabulary and code. Sorting a
temporary copy by `(observation_vocabulary, observation_concept_code)` reduced
their observation scans from 528, 484 and 350 partitions to 16, 7 and 5.
It saved another 6.21s across those queries. That is a useful candidate for
frequent interactive code searches, but the scheduled-build benefit is not yet
enough to establish a net saving. Creating the sorted observation copy took
59.99s, including its read and write cost. The incremental sort cost was not
isolated, and automatic clustering maintenance was not measured.

All clustering trial consumer row counts and aggregate hashes matched. No
production clustering keys or materialisations changed. Further person-key
trials should use real selective cohort joins, rather than assuming that a
high-cardinality ID will make full-history joins cheaper. Snowflake describes
the selection and maintenance trade-offs in its
[clustering key guidance](https://docs.snowflake.com/en/user-guide/tables-clustering-keys).

## Repeat the inventory

Use `model_storage.sql` and `scripts/performance/audit_model_reuse.py` to combine
physical storage metadata with current lineage. The script's README gives the
commands and limitations. Inspect query profiles before changing a shortlisted
model. Keep content-comparison queries, CTAS statements and incremental DML
together when measuring the EPD model's total work.

The inventory covered 66 shared staging views and 33 shared materialised models
with at least ten million rows and no explicit physical clustering key. The
script was checked against synthetic lineage and metadata for model-only
consumer counts, development database mapping, view operations, missing storage
metadata and existing clustering. The documented SQL export and audit command
also ran successfully against the current account metadata.
