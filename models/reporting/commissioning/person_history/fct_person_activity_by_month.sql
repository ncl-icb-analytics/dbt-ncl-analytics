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
), gp_encounter_summary as(
    select
        pp.sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'PrimaryCare' as activity_type
        , count(*) as encounters
        , null::number as cost -- TO DO: replace with pricing from reference book according to app type?
        , sum(actual_duration) as duration
    from 
        {{ ref('int_gp_encounters_appt') }} gpa
    left join {{ref('dim_person_pseudo')}} pp on pp.person_id = gpa.person_id
    group by 
        pp.sk_patient_id, date_trunc('month', start_date)
), csds_encounter_summary as(
    select
        sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'CommunityCareContact' as activity_type
        , count(*) as encounters
        , null::number as cost
        , sum(duration) as duration
    from 
        {{ ref('int_csds_encounters') }}
    group by
        sk_patient_id, date_trunc('month', start_date)
)
, combined as(
    select * from ae_encounter_summary
    union all
    select * from apc_encounter_summary
    union all
    select * from op_encounter_summary
    union all
    select * from gp_encounter_summary
    union all
    select * from csds_encounter_summary
)
select 
    sk_patient_id
    , activity_month
    -- label encounters
    , sum(case when activity_type = 'A&E' then encounters else 0 end) as ae_encounters
    , sum(case when activity_type = 'Inpatient' then encounters else 0 end) as ip_encounters
    , sum(case when activity_type = 'Outpatient' then encounters else 0 end) as op_encounters
    , sum(case when activity_type = 'PrimaryCare' then encounters else 0 end) as gp_encounters
    , sum(case when activity_type = 'CommunityCareContact' then encounters else 0 end) as cc_encounters
    -- label cost
    , sum(case when activity_type = 'A&E' then cost else 0 end) as ae_cost
    , sum(case when activity_type = 'Inpatient' then cost else 0 end) as ip_cost
    , sum(case when activity_type = 'Outpatient' then cost else 0 end) as op_cost
    , sum(case when activity_type = 'CommunityCareContact' then cost else 0 end) as cc_cost
    -- label duration
    , sum(case when activity_type = 'A&E' then duration else 0 end) as ae_duration
    , sum(case when activity_type = 'Inpatient' then duration else 0 end) as ip_duration
    , sum(case when activity_type = 'Outpatient' then duration else 0 end) as op_duration
    , sum(case when activity_type = 'PrimaryCare' then duration else 0 end) as gp_duration
    , sum(case when activity_type = 'CommunityCareContact' then duration else 0 end) as cc_duration
from 
    combined
group by 
    sk_patient_id, activity_month