{{
    config(materialized = 'view')
}}
select
    sk_encounter_id,
    sk_costing_algorithm_id,
    sk_sus_data_mart_id,
    unbundled_no,
    month_of_attendance,
    provider_code,
    purchaser_code,
    contract_suffix,
    is_national,
    base_cost,
    mff,
    apply_mff,
    total_cost,
    hrg_code,
    pod_description,
    schedule_description,
    local_cost_code
from {{ ref('raw_sus_op_monthly_encounterunbundledrepriced') }}
