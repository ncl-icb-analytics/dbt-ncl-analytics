{{
    config(
        materialized='table',
        alias='covid_flu_dashboard_base',
        tags=['secondary_use_opt_out'],
        cluster_by=['programme_type', 'campaign_id', 'practice_code', 'person_id']
    )
}}

/*
COVID and Flu Dashboard Base Table - Secondary Use

Secondary use version with opt-out filtering applied.
Only includes patients allowed for secondary use via dim_person_secondary_use_allowed.

See covid_flu_dashboard_base in published_reporting_direct_care for full documentation.
*/

SELECT base.*
FROM {{ ref('covid_flu_dashboard_base') }} base
INNER JOIN {{ ref('dim_person_secondary_use_allowed') }} allowed
    ON base.person_id = allowed.person_id
