-- Per-person patient-attributable spend over the latest 12 months. This model
-- owns the rolling resource-to-need window over the full-history
-- fct_person_cost_by_month fact.
{{ config(materialized = 'table') }}

with bounds as (
    select max(activity_month) as max_month
    from {{ ref('fct_person_cost_by_month') }}
    where cost_source = 'SLAM'
)

select
    f.sk_patient_id,
    sum(f.total_cost) as actual_cost_12m
from {{ ref('fct_person_cost_by_month') }} as f
cross join bounds as b
where f.is_patient_attributable
  and f.sk_patient_id is not null
  and f.activity_month between dateadd(month, -11, b.max_month) and b.max_month
group by 1
