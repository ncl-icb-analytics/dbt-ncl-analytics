{{
    config(materialized = 'view')
}}

select
    sk_encounter_id,
    provider_commissioner_code,
    provider_practice_code,
    sus_commissioner_code_gp_practice,
    sus_commissioner_code_residence,
    sus_practice_code,
    sollis_commissioner_code,
    dwh_commissioner_code,
    dwh_practice_code,
    provider_gmp_code,
    sk_commissioner_id_dwh_commissioner,
    sk_service_provider_id_dwh_practice,
    activity_period
from {{ ref('raw_sus_op_monthly_encounterbillingattribution') }}
