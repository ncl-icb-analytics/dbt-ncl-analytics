{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'opt_out'],
        cluster_by=['person_id'])
}}

/*
Persons allowed for secondary use.
Central opt-out filter - excludes patients with active Type 1 opt-outs.
All secondary use models should inner join to this to apply opt-out filtering.

Future opt-outs can be added here (e.g., national data opt-out).
*/

WITH opted_out_type_1 AS (
    SELECT person_id
    FROM {{ ref('dim_person_opt_out_type_1_status') }}
    WHERE is_opted_out = TRUE
),

all_persons AS (
    SELECT DISTINCT person_id
    FROM {{ ref('int_patient_person_unique') }}
)

SELECT
    person_id,
    TRUE AS is_allowed_secondary_use
FROM all_persons
WHERE person_id NOT IN (SELECT person_id FROM opted_out_type_1)
