# Snowflake Semantic Views Implementation Plan

## Overview

Snowflake Semantic Views provide a semantic layer directly in Snowflake, enabling:
- **Cortex Analyst integration** — natural language querying of your data
- **Consistent metric definitions** — centralized business logic
- **Self-service analytics** — analysts can query without knowing table joins
- **Governed semantics** — single source of truth for measures/dimensions

You already have the `Snowflake-Labs/dbt_semantic_view` package (v1.0.3) installed.

---

## Current State Analysis

### Model Inventory
| Layer | Models | Purpose |
|-------|--------|---------|
| `models/reporting/` | 174 | Marts layer (facts + dimensions) |
| `models/published/` | 59 | Consumer-facing outputs |

### Key Reporting Domains
| Domain | Model Count | Examples |
|--------|-------------|----------|
| Case Finding (cf) | 28 | Diabetes, CVD risk, SMI casefinding |
| QOF Point-in-Time | 21 | QOF register snapshots |
| QOF Registers | 21 | Disease registers |
| Disease Registers | 16 | Conditions, LTC summaries |
| Person Status | 15 | Registration, demographics |
| Person Demographics | 13 | Demographics dimensions |
| Measures | 6 | BP control, diabetes care processes |
| Organisation | 6 | Practice, PCN, ICB dimensions |

---

## Phased Implementation Plan

### Phase 1: Foundation (Week 1-2)
**Goal:** Establish patterns with a single semantic view

#### 1.1 Create Pilot Semantic View: Population Health Overview

Start with a simple but high-value semantic view combining:

```
models/semantic/
└── semantic_population_health.sql
```

**Tables:**
- `dim_person_demographics_basic` — patient demographics
- `dim_person_age` — age calculations
- `fct_person_ltc_summary` — LTC counts per person

**Example structure:**
```sql
{{ config(materialized='semantic_view') }}

TABLES(
  {{ ref('dim_person_demographics_basic') }} AS demographics
    PRIMARY KEY (sk_patient_id)
    COMMENT = 'Core patient demographics including geography and registration',
  
  {{ ref('fct_person_ltc_summary') }} AS ltc_summary
    PRIMARY KEY (person_id)
    COMMENT = 'Long-term condition summary per patient'
)

RELATIONSHIPS(
  demographics.sk_patient_id = ltc_summary.person_id
)

DIMENSIONS(
  demographics.gender COMMENT = 'Patient gender',
  demographics.ethnicity COMMENT = 'Ethnicity grouping',
  demographics.residence_borough COMMENT = 'Local authority of residence',
  demographics.residence_neighbourhood_name COMMENT = 'NCL neighbourhood',
  demographics.residence_imd_decile COMMENT = 'IMD deprivation decile (1=most deprived)',
  demographics.practice_name COMMENT = 'Registered GP practice',
  demographics.pcn_name COMMENT = 'Primary Care Network'
)

FACTS(
  ltc_summary.ltc_count COMMENT = 'Number of long-term conditions'
)

METRICS(
  SUM(ltc_summary.ltc_count) AS total_ltc_count 
    COMMENT = 'Total LTC count across population',
  COUNT(DISTINCT demographics.sk_patient_id) AS patient_count 
    COMMENT = 'Number of patients',
  AVG(ltc_summary.ltc_count) AS avg_ltc_per_patient 
    COMMENT = 'Average LTCs per patient'
)

COMMENT = 'Population health semantic view for NCL registered population'
```

#### 1.2 Establish Conventions

Create `models/semantic/_semantic_conventions.md`:

| Convention | Standard |
|------------|----------|
| Naming | `semantic_<domain>_<subject>.sql` |
| Location | `models/semantic/<domain>/` |
| Primary keys | Always declare on each table |
| Comments | Required on all DIMENSIONS, FACTS, METRICS |
| Grain | Document expected grain in model header |

---

### Phase 2: Core Domains (Week 3-4)
**Goal:** Build semantic views for high-priority domains

#### Priority 1: Disease Registers
```
models/semantic/registers/
├── semantic_hypertension.sql      # HTN register + BP control
├── semantic_diabetes.sql          # DM register + care processes  
├── semantic_ckd.sql               # CKD register + staging
└── semantic_cardiovascular.sql    # CVD + AF + CHD combined
```

Each combines:
- Register fact table (who's on register)
- Relevant measures (control status, care delivery)
- Demographics dimensions (for segmentation)

#### Priority 2: QOF Performance
```
models/semantic/qof/
├── semantic_qof_hypertension.sql  # HYP indicators
├── semantic_qof_diabetes.sql      # DM indicators
└── semantic_qof_overall.sql       # Cross-indicator view
```

---

### Phase 3: Advanced Patterns (Week 5-6)
**Goal:** Complex relationships and derived metrics

#### 3.1 Multi-condition Analysis
```sql
-- semantic_multimorbidity.sql
-- Enables queries like "patients with diabetes AND hypertension by neighbourhood"
```

#### 3.2 Time-based Analysis
```sql
-- semantic_activity_trends.sql  
-- Uses fct_person_activity_by_month for temporal analysis
```

#### 3.3 Programme-specific Views
```sql
-- semantic_screening_coverage.sql
-- Combines breast/bowel/cervical screening
```

---

## Technical Implementation Details

### Directory Structure
```
models/
└── semantic/
    ├── _semantic_conventions.md
    ├── population/
    │   └── semantic_population_health.sql
    ├── registers/
    │   ├── semantic_hypertension.sql
    │   ├── semantic_diabetes.sql
    │   └── ...
    ├── qof/
    │   ├── semantic_qof_hypertension.sql
    │   └── ...
    └── programmes/
        └── semantic_screening.sql
```

### dbt_project.yml Configuration
```yaml
models:
  dbt_ncl_analytics:
    semantic:
      +materialized: semantic_view
      +schema: SEMANTIC  # Dedicated schema for semantic views
```

### Querying Semantic Views

Once deployed, analysts query via `SEMANTIC_VIEW()` function:

```sql
-- Natural aggregation
SELECT *
FROM SEMANTIC_VIEW(
  ANALYTICS.SEMANTIC.SEMANTIC_POPULATION_HEALTH
  METRICS patient_count, avg_ltc_per_patient
  DIMENSIONS residence_borough
)

-- Filtered analysis
SELECT *
FROM SEMANTIC_VIEW(
  ANALYTICS.SEMANTIC.SEMANTIC_HYPERTENSION
  METRICS controlled_pct, patient_count
  DIMENSIONS practice_name, pcn_name
  WHERE residence_imd_decile <= 3
)
```

---

## Migration Considerations

### What NOT to Migrate
- **Staging models** — too granular, not semantic
- **Intermediate models** — implementation detail
- **Raw/source tables** — not curated

### What TO Include
- **Dimension tables** (dim_*) — patient, organisation, geography
- **Fact tables** (fct_*) — registers, measures, activities
- **Pre-aggregated reports** — if useful as semantic source

### Relationship to dbt Semantic Layer
- Snowflake Semantic Views are **separate** from dbt Semantic Layer (MetricFlow)
- They coexist — use Snowflake SVs for Cortex Analyst, dbt SL for other integrations
- Consider defining core metrics in both if needed

---

## Success Criteria

### Phase 1 Complete When:
- [ ] Pilot semantic view deployed to dev
- [ ] Queryable via `SEMANTIC_VIEW()` function
- [ ] Tested with Cortex Analyst (if available)
- [ ] Conventions documented

### Phase 2 Complete When:
- [ ] 4+ disease register semantic views live
- [ ] QOF semantic view covering key indicators
- [ ] Stakeholder demo delivered

### Phase 3 Complete When:
- [ ] Complex multi-table relationships working
- [ ] Time-series analysis patterns established
- [ ] Documentation complete for analysts

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Cortex Analyst not enabled | SVs still useful for governed metric definitions |
| Performance on large joins | Start with pre-joined fact tables, not raw joins |
| Metric definition drift | Single source of truth in semantic view |
| Learning curve for analysts | Provide example queries, training session |

---

## Next Steps

1. **Confirm Snowflake Cortex Analyst access** — needed for natural language queries
2. **Choose pilot domain** — recommend `fct_person_ltc_summary` + demographics
3. **Create `models/semantic/` directory** and pilot model
4. **Test deployment** in dev environment
5. **Iterate** based on feedback

---

## Questions to Resolve

- [ ] Which Snowflake schema for semantic views? (`SEMANTIC`? `MARTS_SEMANTIC`?)
- [ ] Who are the primary consumers? (Analysts? BI tools? Cortex Analyst?)
- [ ] Any existing metric definitions to align with?
- [ ] Governance: who can modify semantic views?
