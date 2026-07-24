# SUS APC Spell Consolidated Migration - Implementation Summary

**Date Completed**: July 24, 2026  
**Branch**: `feat/create-new-model`  
**Commit**: 62ad42a0  
**Status**: ✅ Complete - Ready for Formal Reconciliation Testing

---

## Overview

Complete implementation of SUS (Secondary Uses Service) **Admitted Patient Care (APC) spells** consolidated migration into dbt, achieving **full architectural parity** with the existing SUS OP (outpatient) work completed in the same branch.

The implementation follows an **identical phased approach** to SUS OP but adapted for **spell-level grain** (vs. encounter-level for OP) and includes:
- ✅ Spell-level commissioner attribution
- ✅ Comprehensive business rules engine (25 active rules)
- ✅ SLA contract classification
- ✅ Quality assurance framework with reconciliation reporting
- ✅ Full documentation and deployment guidance

---

## Deliverables Summary

### Phase 1: Documentation & Discovery ✅

**File**: [docs/sus-apc-consolidated-migration.md](docs/sus-apc-consolidated-migration.md)

Comprehensive migration guide covering:
- **Source-flow decision**: Unified APC source from `DATA_LAKE.SUS_UNIFIED_APC`
- **Commissioner reference tables**: 4-table handover file structure for hierarchical lookup
- **Spell vs. Episode grain**: Critical clarification that facts operate at spell level (aggregated from episodes)
- **Business rules & SLA**: Explanation of 25 active rules and contract type classification
- **Spell metrics**: LOS (length of stay), bed days, episode count calculations
- **Known boundaries**: IMD data versioning, sensitive data flagging, overlapping intervals, postcode lookup scale
- **Deployment order**: Step-by-step guide from reference table loading through final reconciliation
- **Testing strategy**: Comprehensive test suite including uniqueness, date logic, commissioner coverage

---

### Phase 2: Macros & Reference Data ✅

#### 2a: Commissioner Assignment Macro
**File**: [macros/commissioning/assign_sus_apc_commissioner.sql](macros/commissioning/assign_sus_apc_commissioner.sql)

Hierarchical spell-level commissioner attribution:
1. **Priority 1 - GP Practice**: Practice code from spell admission record
2. **Priority 2 - LSOA**: Patient LSOA code if practice lookup fails
3. **Priority 3 - Provider**: Spell commissioning provider code (hosted provider scenario)
4. **Priority 4 - Postcode**: Provider postcode fallback (large reference table ~145MB)

Features:
- Date-effective interval handling (admission date must fall within relationship window)
- Overlapping interval determinism: `min(commissioner_code)` for consistent results
- Reversed date range filtering
- Fallback to 'UNKNOWN' with audit trail of lookup method and priority

#### 2b: Post-Processing Macro
**File**: [macros/commissioning/sus_apc_postprocessing.sql](macros/commissioning/sus_apc_postprocessing.sql)

Spell-level transformations:
- **Sensitive flagging**: Private patients, flagged spells, sensitive diagnosis codes
- **POD derivation**: Place of delivery for maternity spells
- **Tariff assignment**: Spell-level tariff by HRG, admin category, LOS, care-home status
- **Care-home detection**: From admission source and discharge destination codes
- **Admin classification**: Elective, private, non-elective emergency/urgent/transfer
- **Admission method classification**: ELECTIVE, EMERGENCY, TRANSFER, OTHER
- **LOS/bed days**: Length of stay calculation; total bed days across episodes
- **Episode aggregation**: Count distinct episodes; select dominant episode data
- **Spell aggregation**: Final roll-up to one row per spell with all metrics

#### 2c: Business Rules & SLA Seed Data
**File**: [seeds/sus_apc_business_rules_sla.csv](seeds/sus_apc_business_rules_sla.csv) (25 rows)  
**Config**: [seeds/sus_apc_business_rules_sla.yml](seeds/sus_apc_business_rules_sla.yml)

25 active business rules covering:
- **Length of Stay**: Elective short/standard/long-stay, Emergency same-day/short/standard/long-stay
- **Clinical categories**: Mental health, trauma, maternity (normal/complicated), cancer, rehabilitation, palliative
- **Admission/discharge**: Care-home admission, care-home discharge
- **Procedures**: Diagnostic, surgical, medical management, high-cost, specialist/regional services
- **Flags**: Readmission, high bed day (30+ days)

Each rule includes:
- Rule ID, name, detailed description
- Rule type (length-of-stay, clinical-category, admission-source, etc.)
- Admission type applicability (ELECTIVE, NON_ELECTIVE_EMERGENCY, ALL)
- LOS thresholds (min/max days)
- HRG category filter (ALL = applies to all HRGs)
- Effective date range (FY 1314-1718 covered)
- Active flag
- SLA contract type output

---

### Phase 3: Intermediate Spell Aggregation ✅

**File**: [models/modelling/commissioning/sus_encounter/int_sus_apc_spell_monthly.sql](models/modelling/commissioning/sus_encounter/int_sus_apc_spell_monthly.sql)  
**Config**: [models/modelling/commissioning/sus_encounter/int_sus_apc_spell_monthly.yml](models/modelling/commissioning/sus_encounter/int_sus_apc_spell_monthly.yml)

Intermediate-layer model combining:
1. Base spell episodes from `stg_sus_apc_spell_episodes`
2. **Commissioner assignment** via macro with 4-level lookup
3. **Post-processing transformations** via macro
4. **Episode metrics aggregation** (count, total bed days, dominant episode details)
5. **Business rules application** (lateral join to seed matching LOS thresholds, admission type, HRG, effective dates)
6. **SLA contract classification** (contract type from matching rules)

**Output**: One row per spell with:
- Spell identifiers (ID, patient ID, local identifier)
- Dates (admission, discharge, financial year)
- Metrics (LOS, bed days, episode count)
- Commissioner info (code, lookup method, priority)
- Classification (admin category, admission method, HRG)
- Processing flags (sensitive, care-home, POD, tariff)
- Business rules audit trail (pipe-delimited rule names)
- Contract type classification

**Tests**:
- Uniqueness on spell_id
- Not null on critical identifiers
- Date logic: admission ≤ discharge
- Positive LOS
- Row count expectations

---

### Phase 4: Fact Tables ✅

#### 4a: Monthly Fact Table (Full)
**File**: [models/reporting/commissioning/events/fct_sus_apc_monthly.sql](models/reporting/commissioning/events/fct_sus_apc_monthly.sql)  
**Config**: [models/reporting/commissioning/events/fct_sus_apc_monthly.yml](models/reporting/commissioning/events/fct_sus_apc_monthly.yml)

Comprehensive monthly fact table:
- **Grain**: One row per spell per month
- **Scope**: All spells (commissioning + private) for complete audit trail
- **Enrichments**:
  - Provider dictionary lookup (name, code, type)
  - Patient demographics (DOB, age, gender, ethnicity)
  - Calendar dimensions (year, month, quarter, financial year)
  - Data quality flags (future dates, invalid age, missing commissioner, missing HRG)

**Key columns**:
- `sk_spell_id`: Surrogate key (spell_id + FY)
- `spell_id`, `sk_patient_id`, `local_patient_identifier`
- `spell_admission_date`, `spell_discharge_date`
- Metrics: `length_of_stay_days`, `total_bed_days`, `episode_count`
- **Commissioner**: `zcommissioner_code`, `commissioner_lookup_method`, `commissioner_lookup_priority`
- **Classification**: `administrative_category_classification`, `admission_method_classification`
- **Care**: `care_home_flag`, `discharged_to_care_home_flag`, `spell_hrg_code`, `spell_tariff_value`
- **Business logic**: `zbusinessrule` (pipe-delimited), `zcontracttype`
- **Quality**: `future_date_flag`, `invalid_age_flag`, `missing_commissioner_flag`, `missing_hrg_flag`
- **Audit**: `source_extract_type`, `zdatatype` ('SPELL_UNIFIED')

#### 4b: Consolidated Fact Table (Published)
**File**: [models/reporting/commissioning/events/fct_sus_apc_consolidated.sql](models/reporting/commissioning/events/fct_sus_apc_consolidated.sql)

Published fact table for downstream consumers:
- **Grain**: One row per spell (filtered subset)
- **Filters**:
  - Commissioning access only: `zcommissioning_access = 1`
  - Excludes private activity: `administrative_category != 'PRIVATE'`
  - Excludes flagged records: `sensitive_spell_flag = 0`
- **Provider resolution**: Current-care organisation via merger mapping
- **Columns**: Subset of key business metrics (60+ columns trimmed to 28 core columns)
- **Test**: Validates private activity exclusion via expression test

---

### Phase 5: QA & Reconciliation Framework ✅

**File**: [scripts/qa/generate_sus_apc_reconciliation_report.ps1](scripts/qa/generate_sus_apc_reconciliation_report.ps1)

PowerShell-based comprehensive QA reporting script generating **HTML reports** with:

**1. Row Count Reconciliation**
- Compares dbt `fct_sus_apc_monthly` spell count vs. legacy SQL Server output
- Shows unique spell counts
- Targets: Row count variance < 5%

**2. Commissioner Attribution Analysis**
- Distribution of spells across commissioners
- Compares dbt output vs. legacy implementation
- Top 10 commissioners by volume
- % of total for each commissioner

**3. Business Rules Sampling**
- 50-spell random sample with applied rules
- Shows: spell ID, admission date, LOS, category, HRG, business rules applied, contract type
- Validates rule matching logic accuracy

**4. SLA Contract Type Distribution**
- Distribution of contract types in monthly output
- Shows: spell count, % of total, average LOS
- Earliest and latest spell dates

**5. Spell Metrics Validation**
- Statistical summary of key metrics:
  - Length of stay (days): min, avg, median, max
  - Total bed days: min, avg, median, max
  - Episode count: min, avg, median, max
- Identifies outliers and data anomalies

**6. Data Quality Checks**
- Future date issues (spells with admission > today)
- Invalid age issues (patients age outside [0, 120] range)
- Missing commissioner issues (commissioner_code = 'UNKNOWN')
- Missing HRG issues (spell_hrg_code IS NULL)
- Shows count and % of records flagged

**7. HTML Report Features**
- Professional styled report with sections
- Executive summary
- Status indicators (✓ Pass, ⚠ Warning, ✗ Error)
- Reconciliation checkpoints (Phases 1-6 status)
- Next steps for formal testing
- Snowflake connectivity validation
- Parameterized by financial year, sample size

**Usage**:
```powershell
.\scripts\qa\generate_sus_apc_reconciliation_report.ps1 `
  -SnowflakeAccount 'xy12345.eu-west-1' `
  -SnowflakeWarehouse 'COMPUTE_WH' `
  -FinancialYears '2023/24,2022/23' `
  -OutputPath './reports/sus_apc_reconciliation_report.html'
```

---

## File Structure & Changes

```
docs/
  ├── sus-apc-consolidated-migration.md (NEW) - Migration guide

macros/commissioning/
  ├── assign_sus_apc_commissioner.sql (NEW) - Commissioner attribution
  ├── sus_apc_postprocessing.sql (NEW) - Spell transformations

models/modelling/commissioning/sus_encounter/
  ├── int_sus_apc_spell_monthly.sql (NEW) - Intermediate aggregation
  ├── int_sus_apc_spell_monthly.yml (NEW) - Model documentation & tests

models/reporting/commissioning/events/
  ├── fct_sus_apc_monthly.sql (NEW) - Full monthly fact
  ├── fct_sus_apc_monthly.yml (NEW) - Fact documentation & tests
  ├── fct_sus_apc_consolidated.sql (NEW) - Published consolidated fact

seeds/
  ├── sus_apc_business_rules_sla.csv (NEW) - 25 business rules
  ├── sus_apc_business_rules_sla.yml (NEW) - Seed configuration

scripts/qa/
  ├── generate_sus_apc_reconciliation_report.ps1 (NEW) - QA reporting
```

**Total files created**: 11  
**Total lines of code**: ~2,300+ (SQL, YAML, PowerShell)

---

## Key Design Decisions & Patterns

### 1. Spell-Level Grain
- **Decision**: Fact tables operate at spell level (one row per spell), not episode level
- **Rationale**: Spells are the natural business entity for APC; episodes are implementation details
- **Implementation**: `int_sus_apc_spell_monthly` aggregates episodes, using `row_number()` to select one row per spell
- **Impact**: Simplifies metrics (LOS is spell-level, not per-episode); supports spell-level tariffs

### 2. Commissioner Attribution Hierarchy
- **Decision**: 4-level hierarchical lookup (Practice → LSOA → Provider → Postcode) with strict priority
- **Rationale**: Matches legacy SQL Server logic and business requirements for different scenarios
- **Implementation**: Separate lookup CTEs for each priority, combined with UNION ALL and row_number filtering
- **Determinism**: Overlapping intervals resolved via `min(commissioner_code)` to ensure repeatable results
- **Fallback**: 'UNKNOWN' for spells with no commissioner mapping (flagged in fact table)

### 3. Business Rules as Data
- **Decision**: Rules stored in CSV seed file (not hardcoded in SQL)
- **Rationale**: Flexibility for business rule changes; easy audit trail; supports historical rule versioning
- **Implementation**: Lateral join with business rules seed on LOS threshold, admission type, HRG, effective dates
- **Output**: Pipe-delimited `zbusinessrule` column captures all matched rules for audit
- **SLA classification**: Contract type determined by first matching rule (could be extended to weighted scoring)

### 4. Two-Tier Fact Tables
- **Decision**: `fct_sus_apc_monthly` (full) + `fct_sus_apc_consolidated` (filtered)
- **Rationale**: 
  - Monthly = complete audit trail (includes private activity, debugging data)
  - Consolidated = published output (commissioning access only, clean for reporting)
- **Implementation**: Consolidated fact filters monthly fact and adds provider merger resolution
- **Consumer guidance**: Documentation recommends consolidated for standard reporting; monthly for detailed analysis

### 5. Data Quality Flags
- **Decision**: Include quality flag columns in fact table (not separate quality dimension)
- **Rationale**: Immediate visibility of data issues; easier to filter/report on quality
- **Implementation**: Calculated in fact table SELECT with CASE logic
- **Flags**: Future dates, invalid age, missing commissioner, missing HRG
- **Usage**: Identified via test failures; consumed by reconciliation report

### 6. Macro-Based Transformations
- **Decision**: Post-processing logic in macros (not separate intermediate models)
- **Rationale**: Reduces model proliferation; improves maintainability; mirrors SUS OP pattern
- **Implementation**: Two macros (`assign_sus_apc_commissioner`, `sus_apc_postprocessing`) called from `int_sus_apc_spell_monthly`
- **Reusability**: Macros can be reused for other APC models if needed

---

## Reconciliation Strategy

### Checkpoint 1: Row Count Reconciliation ⚠️ (Pending)
- Compare spell counts: dbt output vs. legacy SQL Server
- Target: < 5% variance for closed historical month
- Test period: At least 2 months (1 closed + 1 latest complete)

### Checkpoint 2: Commissioner Distribution ⚠️ (Pending)
- Sample 20-30 spells across commissioners
- Manually verify commissioner attribution logic
- Compare distribution pie chart between dbt and legacy
- Target: ≤ 2% variance per commissioner

### Checkpoint 3: Business Rules Validation ⚠️ (Pending)
- Run legacy rule engine on 30-spell sample
- Compare `zbusinessrule` and `zcontracttype` columns
- Verify rule logic accuracy and SLA classification
- Check for rule combinations that make sense (no contradictions)

### Checkpoint 4: Spell Metrics Reconciliation ⚠️ (Pending)
- Spot-check 50+ spells for:
  - LOS calculation: (discharge_date - admission_date)
  - Episode count: Count of distinct episodes per spell
  - Bed days arithmetic (if applicable)
- Compare against source data

### Checkpoint 5: Data Quality Validation ⚠️ (Pending)
- Run reconciliation report and identify any quality flags
- Investigate future-dated spells (if any)
- Investigate invalid ages (outside [0, 120])
- Verify missing commissioner/HRG spells are legitimate gaps

### Checkpoint 6: Contract Type Distribution ⚠️ (Pending)
- Compare SLA contract type distribution vs. legacy
- Verify all expected contract types present
- Check distribution percentages (< 5% variance per type)

### Checkpoint 7: Documentation & Approval ⚠️ (Pending)
- Final review of migration guide
- Sign-off from business owner (Commissioning Analytics)
- Approval for production publication

**Status**: Phases 1-5 ✅ Complete. **Phase 6 (Formal Testing) Pending** - requires comparison against legacy output

---

## Deployment Instructions

### Pre-Deployment Checklist
- [ ] Verify 4 commissioner reference tables loaded in `DATA_LAKE__NCL.ANALYST_MANAGED`
- [ ] Confirm reference table column names match documentation
- [ ] Validate date ranges in commissioner tables (no data gaps for target period)

### Deployment Steps

1. **Register manual sources**
   ```bash
   # Ensure manual APC sources registered in:
   # - scripts/sources/source_mappings.yml
   # - models/sources/manual_sus_commissioner.yml
   ```

2. **Run source generation pipeline**
   ```bash
   dbt run-operation generate_sources --args '{directory: models/sources}'
   ```

3. **Load business rules seed**
   ```bash
   dbt seed --select sus_apc_business_rules_sla
   ```

4. **Build staging & intermediate models**
   ```bash
   dbt build --select +stg_sus_apc_* +int_sus_apc_spell_monthly
   ```

5. **Build fact tables** (with leading `+` to rebuild intermediate)
   ```bash
   dbt build --select +fct_sus_apc_monthly fct_sus_apc_consolidated
   ```

6. **Validate build**
   ```bash
   dbt test --select fct_sus_apc_monthly fct_sus_apc_consolidated
   ```

7. **Generate reconciliation report**
   ```powershell
   .\scripts\qa\generate_sus_apc_reconciliation_report.ps1 `
     -SnowflakeAccount 'account-name' `
     -SnowflakeWarehouse 'compute_wh' `
     -FinancialYears '2023/24,2022/23'
   ```

8. **Conduct formal reconciliation** (see Checkpoint steps above)

9. **Publish to downstream consumers**
   - Create downstream views/reports referencing `fct_sus_apc_consolidated`
   - Update documentation with availability
   - Communicate to stakeholders

---

## Known Limitations & Future Work

### Current Limitations
1. **IMD version mismatch**: Legacy IMD 2015 fields not currently mapped; placeholder fields in place
2. **Postcode lookup size**: Provider-postcode reference (~145 MB) may impact query performance on large date ranges
3. **Sensitive flagging**: Edge cases in sensitive spell identification may differ from legacy
4. **Overlapping intervals**: Small number of overlapping date ranges resolved deterministically via `min()` - may not match legacy prioritization

### Future Enhancements
1. **Historical business rule versioning**: Support rule changes effective-dated within financial years
2. **Provider merger tracking**: Enhanced `stg_dictionary_organisation_mergers` for historical provider name resolution
3. **Performance optimization**: Partitioning by financial year, incremental models for large spell volumes
4. **Extended QA framework**: Automated anomaly detection, Monte Carlo sampling for larger datasets
5. **Real-time updates**: Change Data Capture (CDC) for near-real-time spell updates vs. current monthly batches

---

## Testing & Validation

### Unit Tests (dbt Tests)
- Uniqueness on `spell_id`
- Not null on critical identifiers
- Date logic: `spell_admission_date <= spell_discharge_date`
- Positive LOS: `length_of_stay_days >= 0`
- Expression: `coalesce(administrative_category, '00') not in ('02', '2')` (exclude private in consolidated)
- Row count > 1 for both fact tables

### Integration Tests (Manual)
- Run reconciliation report (generates HTML)
- Compare row counts: dbt vs. legacy
- Spot-check commissioning attribution logic
- Validate business rules on sample spells
- Verify metrics (LOS, bed days, episode counts)

### Performance Tests (Post-Deployment)
- Query performance on monthly fact tables
- Index recommendations for fact table joins
- ETL runtime monitoring (dbt model execution times)

---

## Branch Information

**Branch**: `feat/create-new-model`  
**Previous Commits**: SUS OP migration work (OP outpatient equivalent)  
**Current Commit**: 62ad42a0 (SUS APC spell migration - complete implementation)

**Related Files in Branch**:
- SUS OP migration: `docs/sus-op-consolidated-migration.md`, `fct_sus_op_monthly`, `fct_sus_op_consolidated`
- SUS APC migration: This implementation

Both feature SUS OP and APC can be merged to main after reconciliation testing.

---

## Summary

✅ **Complete SUS APC spell-level fact table implementation** with:
- Full architectural parity to SUS OP work
- Spell-level grain with episode aggregation
- Hierarchical commissioner attribution (4-level lookup)
- 25 active business rules with SLA classification
- Comprehensive QA and reconciliation framework
- Professional documentation and deployment guides

⚠️ **Pending**: Formal reconciliation testing against legacy SQL Server output

🚀 **Status**: Ready for reconciliation validation; pending business sign-off for production publication

---

**Implementation Lead**: Dharmesh Kumar  
**Date Completed**: July 24, 2026  
**Branch**: feat/create-new-model  
**Contact**: Commissioning Analytics Team
