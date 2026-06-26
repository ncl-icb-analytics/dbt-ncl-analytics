-- Per-person patient-attributable spend over the latest 12 months, from
-- fct_person_cost_by_month (is_patient_attributable rows only). Building block
-- shared by the age-sex cost curve and the person resource-vs-need fact.
{{ config(materialized = 'table') }}

with bounds as (
    select max(activity_month) as max_month
    from {{ ref('fct_person_cost_by_month') }}
)

select
    f.sk_patient_id,
    sum(f.total_cost) as actual_cost_12m
from {{ ref('fct_person_cost_by_month') }} as f
cross join bounds as b
where f.is_patient_attributable
  and f.sk_patient_id is not null
  and f.activity_month >= dateadd(month, -11, b.max_month)
group by 1
