{{
    config(materialized = 'view')
}}
select
    sk_encounter_id,
    provider_code_sollis_derived,
    commissioner_code_sollis_derived,
    provider_code_provider_derived,
    commissioner_code_provider_derived,
    commissioner_code_derived_from_gp_code,
    commissioner_code_derived_from_practice_code,
    sus_commissioner_code_derived_from_residence,
    activity_period
from {{ ref('raw_sus_op_monthly_encounterattribution') }}
