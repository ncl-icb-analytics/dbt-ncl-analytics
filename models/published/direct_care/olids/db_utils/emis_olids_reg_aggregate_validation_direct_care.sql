{{
    config(
        materialized='table',
        alias='emis_olids_reg_aggregate_validation',
        tags=['data_quality', 'published', 'direct_care']
    )
}}

/*
EMIS/OLIDS Aggregate Validation (Direct Care)

Aggregate-level validation of OLIDS Regular registrations against EMIS list size.
Shows whether the overall system meets the <1% variance acceptance criteria.

Acceptance Criteria: <1% variance in aggregate across all 175 practices

Use Cases:
- System-wide data quality monitoring
- Monthly validation reporting
- Acceptance criteria compliance tracking
- Complement to per-practice validation

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED schema
- Table alias: emis_olids_aggregate_validation
- Single row showing aggregate statistics and PASS/FAIL status
*/

select
    practice_count,
    total_emis_list_size,
    total_olids_regular_count,
    aggregate_difference,
    aggregate_percent_difference,
    aggregate_absolute_percent_difference,
    aggregate_acceptance_status,
    snapshot_date,
    validation_timestamp
from {{ ref('int_emis_olids_aggregate_validation') }}
