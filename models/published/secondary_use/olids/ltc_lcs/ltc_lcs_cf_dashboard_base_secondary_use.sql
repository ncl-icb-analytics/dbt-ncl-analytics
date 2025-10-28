{{
    config(
        materialized='table',
        alias='ltc_lcs_cf_dashboard_base',
        tags=['secondary_use_opt_out'],
        cluster_by=['indicator_category', 'indicator_id']
    )
}}

/*
LTC/LCS Case Finding Dashboard Base Table - Secondary Use

Secondary use version with opt-out filtering applied.
Only includes patients allowed for secondary use via dim_person_secondary_use_allowed.

See ltc_lcs_cf_dashboard_base in published_reporting_direct_care for full documentation.
*/

SELECT base.*
FROM {{ ref('ltc_lcs_cf_dashboard_base') }} base
INNER JOIN {{ ref('dim_person_secondary_use_allowed') }} allowed
    ON base.person_id = allowed.person_id
