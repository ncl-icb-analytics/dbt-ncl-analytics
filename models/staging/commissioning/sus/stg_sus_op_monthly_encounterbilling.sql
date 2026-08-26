{{
    config(materialized = 'view')
}}

select
    sk_encounter_id,
    sk_service_provider_id,
    sk_commissioner_id,
    schedule_code,
    contract_suffix,
    encounter_row_id,
    total_cost,
    mff_applied,
    is_short_stay,
    long_stay_payment,
    service_adjustment_applied,
    critical_care_day_count,
    applicable_tariff,
    sk_date,
    base_cost,
    schedule_description,
    pod_description,
    local_cost_code,
    pbr_final_tariff,
    sk_tariff_type_id,
    is_pbr,
    hrg_code,
    activity_period
from {{ ref('raw_sus_op_monthly_encounterbilling') }}
