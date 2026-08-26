{{
    config(materialized = 'view')
}}

select
    sk_encounter_id,
    sk_costing_algorithm_id,
    sk_sus_data_mart_id,
    month_of_attendance,
    provider_code,
    purchaser_code,
    contract_suffix,
    base_cost,
    total_cost,
    mff_applied,
    pod_description,
    schedule_description,
    local_cost_code,
    hrg_code,
    service_line
from {{ ref('raw_sus_op_monthly_encounterbillingrepriced')}}
