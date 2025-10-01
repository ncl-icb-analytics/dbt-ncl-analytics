{{ config(materialized='table') }}

/*
Monthly summary of patient activity

Clinical Purpose:
- Tracking outcomes and activity over time
- Measuring impact of interventions

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

with ae_encounter_summary as(
    select
        sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'A&E' as activity_type
        , count(*) as encounters
        , sum(cost) as cost
        , sum(duration) as duration
    from 
        {{ ref('int_sus_ae_encounters') }}
    group by 
        sk_patient_id, date_trunc('month', start_date)
)
, apc_encounter_summary as(
    select
        sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'Inpatient' as activity_type
        -- TO DO: add elective vs non-elective (POD) breakdown
        , count(*) as encounters
        , sum(cost) as cost
        , sum(duration) as duration
    from 
        {{ ref('int_sus_apc_encounters') }}
    group by 
        sk_patient_id, date_trunc('month', start_date)
)
, op_encounter_summary as(
    select
        sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'Outpatient' as activity_type
        -- TO DO: add first appointment vs follow-up breakdown
        , count(*) as encounters
        , sum(cost) as cost
        , sum(expected_duration) as duration
    from 
        {{ ref('int_sus_op_encounters') }}
    group by 
        sk_patient_id, date_trunc('month', start_date)
)
, combined as(
    select * from ae_encounter_summary
    union all
    select * from apc_encounter_summary
    union all
    select * from op_encounter_summary
)
select 
    sk_patient_id
    , activity_month
    , sum(case when activity_type = 'A&E' then encounters else 0 end) as ae_encounters
    , sum(case when activity_type = 'Inpatient' then encounters else 0 end) as ip_encounters
    , sum(case when activity_type = 'Outpatient' then encounters else 0 end) as op_encounters
    , sum(case when activity_type = 'A&E' then cost else 0 end) as ae_cost
    , sum(case when activity_type = 'Inpatient' then cost else 0 end) as ip_cost
    , sum(case when activity_type = 'Outpatient' then cost else 0 end) as op_cost
    , sum(case when activity_type = 'A&E' then duration else 0 end) as ae_duration
    , sum(case when activity_type = 'Inpatient' then duration else 0 end) as ip_duration
    , sum(case when activity_type = 'Outpatient' then duration else 0 end) as op_duration
from 
    combined
group by 
    sk_patient_id, activity_month