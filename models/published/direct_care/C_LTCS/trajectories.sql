{{
    config(
        materialized='table')
}}


/*
Patient trajectory data processing for CLTCS

Clinical Purpose:
- Showing activity changes over time for patients with complex needs

Testing:
- Actual table will use full array of datasets and include measurement observations

*/

with date_spine as(
    select add_months(date_trunc('month',  dateadd(month, -18, current_date())),seq4())::date as activity_month
    FROM TABLE(GENERATOR(ROWCOUNT => 18))
    order by activity_month
),
inclusion_list as(
    select distinct patient_id
    from {{ ref('inclusion_cohort')}}
    where eligible = 1
), 
activity as(
    select sk_patient_id as patient_id
    , activity_month
    , ae_encounters
    , ip_encounters
    , op_encounters
    , gp_encounters
    from {{ref('fct_person_activity_by_month')}}
    where activity_month between dateadd(month, -13, current_date()) and current_date()
)

select il.patient_id
    ,ARRAY_AGG(COALESCE(a.ae_encounters,0)) within group (order by ds.activity_month) as ae_encounters_sl
    ,ARRAY_AGG(COALESCE(a.ip_encounters,0)) within group (order by ds.activity_month) as ip_encounters_sl
    ,ARRAY_AGG(COALESCE(a.op_encounters,0)) within group (order by ds.activity_month) as op_encounters_sl
    ,ARRAY_AGG(COALESCE(a.gp_encounters,0)) within group (order by ds.activity_month) as gp_encounters_sl
from inclusion_list il
cross join date_spine ds
left join activity a 
    on il.patient_id = a.patient_id and ds.activity_month = a.activity_month
group by il.patient_id

