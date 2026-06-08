{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'opt_out'],
        cluster_by=['person_id'],
        meta={
            'custom_message': 'Opt-out filter dim at person_id grain. INNER JOIN to this from OLIDS models keyed by person_id to apply National Data Opt-Out and Type 1 opt-out filtering. Built from int_person_ndoo_status and dim_person_opt_out_type_1_status.'
        })
}}

/*
Persons allowed for secondary use.
Central opt-out filter. Included only when BOTH:
  - National Data Opt-Out preference is OPT_IN (int_person_ndoo_status), AND
  - No active Type 1 opt-out from clinical observations (dim_person_opt_out_type_1_status).
All secondary use models should inner join to this to apply opt-out filtering.
*/

WITH ndoo_opt_in AS (
    SELECT person_id
    FROM {{ ref('int_person_ndoo_status') }}
    WHERE preference_type = 'NATIONAL_DATA_OPT_OUT'
        AND is_opted_in = TRUE
),

opted_out_type_1 AS (
    SELECT person_id
    FROM {{ ref('dim_person_opt_out_type_1_status') }}
    WHERE is_opted_out = TRUE
)

SELECT
    person_id,
    TRUE AS is_allowed_secondary_use
FROM ndoo_opt_in
WHERE person_id NOT IN (SELECT person_id FROM opted_out_type_1)
