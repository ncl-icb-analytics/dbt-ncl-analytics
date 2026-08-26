{{
    config(materialized = 'view')
}}
select
    sk_encounter_id,
    hrg,
    unbundled_no,
    pod_description,
    schedule_description,
    provider,
    purchaser,
    suffix,
    cost,
    cost_code,
    is_national,
    month_of_activity,
    base_tariff,
    mff,
    apply_mff,
    tariff_type,
    activity_period
from {{ ref('raw_sus_op_monthly_encounterunbundled') }}
