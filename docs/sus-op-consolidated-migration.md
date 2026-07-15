# SUS outpatient consolidated migration

## Source-flow decision

The dbt model reads `DATA_LAKE.SUS_OP.EncounterDenormalised_DateRange`. That
source contains the historical date ranges that were formerly assembled from
the separate `Current`, `Rec` and `PostRec` SQL Server implementations. The dbt
flow therefore remains a single source flow; it does not recreate three parallel
model branches. `source_extract_type` is retained for audit and `zdatatype` is
set to `DATE_RANGE` rather than incorrectly labelling every row `CURRENT`.

## Commissioner reference tables

The four handover files must be loaded by the governed Snowflake ingestion
process into `DATA_LAKE__NCL.ANALYST_MANAGED` with these table and column names.
They are registered as a curated `manual: true` source in
`scripts/sources/source_mappings.yml` and `models/sources/manual_sus_commissioner.yml`,
so the normal source-generation pipeline creates the raw models and reports schema
drift. They are intentionally not dbt seeds.

| Table | Required columns |
| --- | --- |
| `SUS_COMMISSIONER_PRACTICE` | `SK_ORGANISATIONID_PRACTICE`, `ORGANISATIONCODE_PRACTICE`, `SK_ORGANISATIONID_COMMISSIONER`, `ORGANISATIONCODE_COMMISSIONER`, `PRACTICESTARTDATE`, `PRACTICEENDDATE`, `RELATIONSHIPSTARTDATE`, `RELATIONSHIPENDDATE`, `ISACTIVE`, `ISPROXY` |
| `SUS_COMMISSIONER_LSOA` | `OACODE`, `ORGANISATIONCODE_COMMISSIONER`, `EFFECTIVEFROM`, `EFFECTIVETO` |
| `SUS_COMMISSIONER_PROVIDER` | `PROVIDERCODE`, `COMMISSIONERCODE`, `EFFECTIVEFROM`, `EFFECTIVETO` |
| `SUS_COMMISSIONER_PROVIDER_POSTCODE` | `PROVIDERCODE`, `COMMISSIONERCODE`, `EFFECTIVEFROM`, `EFFECTIVETO` |

The provider-postcode reference is approximately 145 MB uncompressed, so it is
deliberately not committed as a dbt seed. The macro preserves the legacy lookup
priority: practice, LSOA, hosted provider, then provider postcode. Invalid
reversed intervals are filtered. A small number of overlapping intervals exist
in the handed-over data; `min(commissioner_code)` makes their result deterministic.

## Migrated transformations

`fct_sus_op_monthly` replaces the OP post-process procedures and the active OP
section of `dbo.processBusinessRules`. It includes duplicate access handling,
sensitive-category mapping, POD derivation, tariff-derived pricing, care-home
assignment, the 25 active OP rules and the legacy SLA contract classification.
The SLA seed was generated from `objectswithdata.sql` and covers the supplied
financial years 1314 through 1718.

`fct_sus_op_consolidated` applies the publish filters from the consolidated view
and resolves the provider's current-care organisation through the organisation
dictionary.

## Known boundary

The legacy output names IMD 2015 fields, but only an IMD 2019 reference is
currently present in this repository. This migration does not silently substitute
a different index release. The existing fields remain unchanged until an exact
IMD 2015 mapping is supplied and agreed.

## Deployment order

1. Load and validate the four commissioner reference tables.
2. Run the normal source-generation pipeline (or its offline steps 2 and 3) so
   manual-source drift is checked and the raw models remain generated artifacts.
3. Run `dbt seed --select sus_op_business_rules_sla`.
4. Build `fct_sus_op_monthly+` so the monthly and consolidated facts are created.
5. Compare row counts, commissioner allocation, business rules and contract types
   with the legacy output for at least one closed historical month and the latest
   complete month before publishing to downstream consumers.
