{{
    config(
        materialized='table',
        alias='childhood_imms_person_level_adolescent_dm',
        tags=['secondary_use_opt_out']
    )
}}

/*
Childhood Immunisations Person Level Adolescent - Secondary Use

Secondary use version with opt-out filtering applied.
Only includes patients allowed for secondary use via dim_person_secondary_use_allowed.

See childhood_imms_person_level_adolescent_dm in published_reporting_direct_care for full documentation.
*/

SELECT base.*
FROM {{ ref('childhood_imms_person_level_adolescent_dm') }} base
INNER JOIN {{ ref('dim_person_secondary_use_allowed') }} allowed
    ON base.person_id = allowed.person_id
