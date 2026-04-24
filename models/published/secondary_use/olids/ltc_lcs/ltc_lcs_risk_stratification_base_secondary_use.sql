{{
    config(
        materialized='table',
        alias='ltc_lcs_risk_stratification_base',
        tags=['secondary_use_opt_out'],
        cluster_by=['overall_risk_rank']
    )
}}

-- LTC LCS Risk Stratification Base Table - Secondary Use
-- Secondary use version with opt-out filtering applied.
-- See ltc_lcs_risk_stratification_base in published_reporting_direct_care for full documentation.

select base.*
from {{ ref('ltc_lcs_risk_stratification_base') }} base
inner join {{ ref('dim_person_secondary_use_allowed') }} allowed
    on base.person_id = allowed.person_id
