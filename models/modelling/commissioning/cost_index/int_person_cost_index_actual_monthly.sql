-- Per-person patient-attributable actual spend by month.
{{ config(materialized = 'table') }}

with slam as (
    select
        sk_patient_id,
        activity_month,
        sum(total_cost) as actual_cost
    from {{ ref('int_cost_index_slam_activity_monthly') }}
    where is_patient_attributable
      and sk_patient_id is not null
      and cost_basis = 'actual'
    group by 1, 2
),

epd as (
    select
        sk_patient_id,
        activity_month,
        sum(prescribing_cost) as actual_cost
    from {{ ref('int_cost_index_epd_prescribing_monthly') }}
    where sk_patient_id is not null
      and cost_basis = 'actual'
    group by 1, 2
)

select
    sk_patient_id,
    activity_month,
    sum(actual_cost) as actual_cost
from (
    select * from slam
    union all
    select * from epd
) as monthly_costs
group by 1, 2
