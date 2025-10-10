{{
    config(
        materialized='table',
        alias='population_health_needs_base_readable',
        tags=['secondary_use_opt_out']
    )
}}

/*
Population Health Needs Base Readable - Secondary Use

Secondary use version with opt-out filtering applied.
Only includes patients allowed for secondary use via dim_person_secondary_use_allowed.

See population_health_needs_base_readable in published_reporting_direct_care for full documentation.
*/

SELECT base.*
FROM {{ ref('population_health_needs_base_readable') }} base
INNER JOIN {{ ref('dim_person_secondary_use_allowed') }} allowed
    ON base.person_id = allowed.person_id
