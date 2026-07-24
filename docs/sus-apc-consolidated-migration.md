# SUS admitted patient care spell consolidated migration

## Source-flow decision

The dbt model reads `DATA_LAKE.SUS_UNIFIED_APC.SpellEpisodes` and related tables. This source contains spell and episode-level data for admitted patient care. The dbt flow processes spells as the primary grain (one row per spell across fact tables), aggregating episode-level information from the source.

`source_extract_type` is retained for audit and `zdatatype` is set to `SPELL_UNIFIED` to identify the unified APC source used. This replaces the legacy separate `Current`, `Rec` and `PostRec` SQL Server implementations with a single consolidated source flow.

## Commissioner reference tables

APC spells are attributed to commissioners based on GP practice, LSOA, or provider organisational codes. The same four handover reference files used for OP are leveraged for APC, with spell-level application. They must be loaded by the governed Snowflake ingestion process into `DATA_LAKE__NCL.ANALYST_MANAGED`.

They are registered as a curated `manual: true` source in `scripts/sources/source_mappings.yml` and `models/sources/manual_sus_commissioner.yml`, so the normal source-generation pipeline creates the raw models and reports schema drift. They are intentionally not dbt seeds.

| Table | Required columns | APC Application |
| --- | --- | --- |
| `SUS_COMMISSIONER_PRACTICE` | `SK_ORGANISATIONID_PRACTICE`, `ORGANISATIONCODE_PRACTICE`, `SK_ORGANISATIONID_COMMISSIONER`, `ORGANISATIONCODE_COMMISSIONER`, `PRACTICESTARTDATE`, `PRACTICEENDDATE`, `RELATIONSHIPSTARTDATE`, `RELATIONSHIPENDDATE`, `ISACTIVE`, `ISPROXY` | Used to attribute spell commissioner via GP practice code (from spell admission record) |
| `SUS_COMMISSIONER_LSOA` | `OACODE`, `ORGANISATIONCODE_COMMISSIONER`, `EFFECTIVEFROM`, `EFFECTIVETO` | Used to attribute spell commissioner via patient LSOA (if practice lookup fails) |
| `SUS_COMMISSIONER_PROVIDER` | `PROVIDERCODE`, `COMMISSIONERCODE`, `EFFECTIVEFROM`, `EFFECTIVETO` | Used to attribute spell commissioner via provider code (hosted provider scenario) |
| `SUS_COMMISSIONER_PROVIDER_POSTCODE` | `PROVIDERCODE`, `COMMISSIONERCODE`, `EFFECTIVEFROM`, `EFFECTIVETO` | Used as fallback for provider-postcode based commissioner lookup |

The provider-postcode reference is approximately 145 MB uncompressed, so it is deliberately not committed as a dbt seed. The macro preserves the lookup priority: practice → LSOA → hosted provider → provider postcode. Invalid reversed date intervals are filtered. A small number of overlapping intervals exist in the handed-over data; `min(commissioner_code)` makes their result deterministic.

## Spell vs. Episode grain clarification

**Critical difference from SUS OP:**
- **SUS OP**: One row per encounter (visit)
- **SUS APC**: One row per **spell** (continuous hospital stay, may contain multiple episodes)

An APC spell represents a continuous hospital stay from admission to discharge. Within a spell, there can be multiple episodes (e.g., ward changes, consultant changes). The fact tables operate at **spell level**, aggregating episode-level information:

- `fct_sus_apc_monthly`: One row per spell per month
- Episode count, total length of stay, and dominant episode details are aggregated at spell level
- Clinical coding (diagnoses, procedures) can have multiple rows if unbundling is applied
- Tariff values are spell-level (not episode-level)
- Business rules are evaluated against spell characteristics (admission type, discharge destination, spell HRG)

## Migrated transformations

`fct_sus_apc_monthly` replaces the APC post-process procedures and the active APC section of the legacy SQL Server processing. It includes:

- **Commissioner attribution**: Practice → LSOA → provider → postcode lookup with date-effective intervals
- **Duplicate episode handling**: Deduplicate to latest transaction date per episode within spell
- **Sensitive category mapping**: Identifier flag for sensitive admission types
- **POD (Place of Delivery) derivation**: For maternity spells
- **Tariff-derived pricing**: Spell-level tariff assignment (may vary by LOS and HRG)
- **Care-home assignment**: Derived from admission source codes
- **Spell-level metrics**: Length of stay (LOS), bed days, episode count, admission type, discharge destination
- **APC business rules**: Active business rules applied at spell level (documented in seed)
- **SLA contract classification**: Legacy SLA contract types by HRG and admission type for financial years

`fct_sus_apc_consolidated` applies the publish filters from the unified view (commissioning access records only), excludes private patient activity, and resolves the provider's current-care organisation through the organisation dictionary.

## Business rules and SLA classification

The APC business rules differ from OP rules in:
- **Admission type filtering**: Elective vs. non-elective vs. emergency distinctions
- **Length of stay thresholds**: Rules may be triggered based on spell LOS (e.g., long-stay flagging)
- **Discharge destination rules**: Post-discharge destination-specific rules
- **HRG-specific rules**: Certain rules apply only to specific HRG groups (mental health, trauma, etc.)
- **Historical SLA contracts**: Contracts are indexed by admission type and HRG for financial years 1314-1718

The SLA seed (`sus_apc_business_rules_sla.csv`) was generated from legacy SUS data and covers all supplied financial years. Business rules are stored as pipe-delimited values in `zbusinessrule` column for audit trail and matching.

## Spell metrics and calculations

- **Length of stay (LOS)**: Calculated as `spell_discharge_date - spell_admission_date` (in days)
- **Bed days**: May be calculated from episode-level bed day data (sum across all episodes in spell)
- **Episode count**: Number of distinct episodes within the spell
- **Dominant episode**: Primary episode in spell (marked by `dominant_episode_flag = 1` in source)
- **Total tariff**: Sum of episode tariffs or spell-level tariff assignment

## Known boundaries and limitations

1. **IMD data**: The legacy output names IMD 2015 fields, but only an IMD 2019 reference is currently present in this repository. This migration does not silently substitute a different index release. Placeholder fields remain unchanged until an exact IMD 2015 mapping is supplied and agreed.

2. **Sensitive data flagging**: Sensitive spell indicators are retained for audit but may not align 100% with legacy flagging logic until business rules are fully documented.

3. **Overlapping intervals in commissioner data**: When date ranges overlap for the same organisation/commissioner combination, `min(commissioner_code)` produces deterministic output. This may differ from legacy prioritisation in edge cases.

4. **Episode deduplication**: Only the latest transaction (by transaction date) is retained per episode per spell. Multiple historical transactions for the same episode are excluded.

5. **Private patient activity**: Identified via administrative category codes ('02', '2') and excluded from `fct_sus_apc_consolidated` but retained in `fct_sus_apc_monthly` for internal audit.

6. **Postcode-based lookups**: The provider-postcode reference table is extremely large (~145 MB). If performance issues arise, consider filtering to recent financial years only or partitioning by date range.

## Deployment order

1. **Load and validate** the four commissioner reference tables via governed ingestion into `DATA_LAKE__NCL.ANALYST_MANAGED`.
2. **Register** manual APC sources in `scripts/sources/source_mappings.yml` and ensure drift detection runs.
3. **Run** the normal source-generation pipeline so manual sources are validated and raw models are created.
4. **Seed** the APC business rules: `dbt seed --select sus_apc_business_rules_sla`.
5. **Build** staging models and the intermediate spell aggregation layer: `dbt build --select +stg_sus_apc_* +int_sus_apc_spell_monthly`.
6. **Build** the fact tables with leading `+` to rebuild intermediate relations: `dbt build --select +fct_sus_apc_monthly fct_sus_apc_consolidated`.
7. **Reconcile** against legacy output for at least one closed historical month and the latest complete month before publishing to downstream consumers:
   - Row count comparison (spells)
   - Commissioner distribution analysis
   - Business rules application sampling
   - Contract type distribution
   - LOS and bed days metrics validation
   - Spell-level date logic checks (admission ≤ episode dates ≤ discharge)

## Testing strategy

Comprehensive test suite includes:
- **Uniqueness**: Spell should be unique (one row per spell) via `dbt_utils.unique_combination_of_columns`
- **Not null**: Essential spell identifiers and dates
- **Expression tests**: Spell start date ≤ spell end date
- **Referential integrity**: Spell IDs link to valid source spells
- **Date logic**: No reversed or impossible date intervals
- **Grain validation**: Confirm aggregation to spell level is complete
- **Commissioner coverage**: All spells have a valid commissioner attribution
- **Business rules coverage**: Active rules for date range are applied

## Migration owner

**Commissioning Analytics** - responsible for validation, testing, and downstream communication of new spell-level fact tables.
