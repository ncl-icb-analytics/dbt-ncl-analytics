{{
    config(
        materialized='view',
        alias='patient_opt_out_summary',
        tags=['data_quality', 'published', 'secondary_use', 'opt_out']
    )
}}

/*
Patient Opt-Out Summary (Secondary Use)

Simple count showing total registered patients and patients allowed for secondary use.
The difference represents patients opted out (any opt-out type).

Future-proof design: As new opt-out types are added to dim_person_secondary_use_allowed,
the counts automatically adjust without code changes.

Use Cases:
- Data governance reporting
- Secondary use eligibility monitoring
- Opt-out compliance tracking
- Monthly reporting on data availability

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__SECONDARY_USE.OLIDS_PUBLISHED schema
- Use for executive dashboards
- Track opt-out trends over time
*/

WITH total_registered_patients AS (
    -- Use dim_person_demographics as the definitive registered patient count
    SELECT COUNT(DISTINCT person_id) as total_count
    FROM {{ ref('dim_person_demographics') }}
),

allowed_secondary_use AS (
    -- Patients allowed for secondary use (from demographics, excluding opt-outs)
    SELECT COUNT(DISTINCT d.person_id) as allowed_count
    FROM {{ ref('dim_person_demographics') }} d
    INNER JOIN {{ ref('dim_person_secondary_use_allowed') }} a
        ON d.person_id = a.person_id
),

opted_out_type_1 AS (
    -- Patients with Type 1 opt-outs (from demographics only)
    SELECT COUNT(DISTINCT d.person_id) as opted_out_count
    FROM {{ ref('dim_person_demographics') }} d
    INNER JOIN {{ ref('dim_person_opt_out_type_1_status') }} o
        ON d.person_id = o.person_id
    WHERE o.is_opted_out = TRUE
)

SELECT
    total_registered_patients.total_count as total_registered_patients,
    allowed_secondary_use.allowed_count as patients_allowed_secondary_use,
    opted_out_type_1.opted_out_count as patients_opted_out_type_1,
    (total_registered_patients.total_count - allowed_secondary_use.allowed_count) as patients_opted_out_all_types,
    ROUND(
        (opted_out_type_1.opted_out_count * 100.0) / NULLIF(total_registered_patients.total_count, 0),
        2
    ) as percent_opted_out_type_1,
    ROUND(
        ((total_registered_patients.total_count - allowed_secondary_use.allowed_count) * 100.0) / NULLIF(total_registered_patients.total_count, 0),
        2
    ) as percent_opted_out_all_types
FROM total_registered_patients
CROSS JOIN allowed_secondary_use
CROSS JOIN opted_out_type_1
