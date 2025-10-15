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
        , pod as activity_subtype
        , count(*) as encounters
        , sum(cost) as cost
        , sum(duration) as duration
    from 
        {{ ref('int_sus_ae_encounters') }}
    where 
        sk_patient_id is not null
    group by 
        sk_patient_id, date_trunc('month', start_date), pod
)
, apc_encounter_summary as(
    select
        sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'Inpatient' as activity_type
        , admission_method_group as activity_subtype
        , count(*) as encounters
        , sum(cost) as cost
        , sum(duration) as duration
    from 
        {{ ref('int_sus_apc_encounters') }}
    where 
        sk_patient_id is not null
    group by 
        sk_patient_id, date_trunc('month', start_date), admission_method_group
)
, op_encounter_summary as(
    select
        sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'Outpatient' as activity_type
        , pod
        , count(*) as encounters
        , sum(cost) as cost
        , sum(expected_duration) as duration
    from 
        {{ ref('int_sus_op_encounters') }}
    where 
        sk_patient_id is not null
    group by 
        sk_patient_id, date_trunc('month', start_date), pod
)
, gp_encounter_summary as(
    select
        pp.sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'PrimaryCare' as activity_type
        , null::varchar as pod
        , count(*) as encounters
        , null::number as cost -- TO DO: replace with pricing from reference book according to app type?
        , sum(actual_duration) as duration
    from 
        {{ ref('int_gp_encounters_appt') }} gpa
    left join {{ref('dim_person_pseudo')}} pp on pp.person_id = gpa.person_id
    group by 
        pp.sk_patient_id, date_trunc('month', start_date)
)
, combined as(
    select * from ae_encounter_summary
    union all
    select * from apc_encounter_summary
    union all
    select * from op_encounter_summary
    union all
    select * from gp_encounter_summary
)
select 
    sk_patient_id
    , activity_month
    , sum(encounters) as total_encounters
    , sum(cost) as total_cost

    -- high level breakdowns
    , sum(case when activity_type = 'A&E' then encounters else 0 end) as ae_encounters
    , sum(case when activity_type = 'Inpatient' then encounters else 0 end) as ip_encounters
    , sum(case when activity_type = 'Outpatient' then encounters else 0 end) as op_encounters
    , sum(case when activity_type = 'PrimaryCare' then encounters else 0 end) as gp_encounters

    , sum(case when activity_type = 'A&E' then cost else 0 end) as ae_cost
    , sum(case when activity_type = 'Inpatient' then cost else 0 end) as ip_cost
    , sum(case when activity_type = 'Outpatient' then cost else 0 end) as op_cost
    , sum(case when activity_type = 'A&E' then duration else 0 end) as ae_duration

    , sum(case when activity_type = 'Inpatient' then duration else 0 end) as ip_duration
    , sum(case when activity_type = 'Outpatient' then duration else 0 end) as op_duration
    , sum(case when activity_type = 'PrimaryCare' then duration else 0 end) as gp_duration

    -- subtype breakdowns
    , sum(case when activity_type = 'A&E' and activity_subtype in ('AE-T1', 'AE-Other') then encounters else 0 end) as ae_emergency_encounters
    , sum(case when activity_type = 'A&E' and activity_subtype in ('UCC', 'WiC', 'SDEC') then encounters else 0 end) as ae_urgent_encounters
    , sum(case when activity_type = 'A&E' and activity_subtype in ('AE-T1', 'AE-Other') then cost else 0 end) as ae_emergency_cost
    , sum(case when activity_type = 'A&E' and activity_subtype in ('UCC', 'WiC', 'SDEC') then cost else 0 end) as ae_urgent_cost
    , sum(case when activity_type = 'A&E' and activity_subtype in ('AE-T1', 'AE-Other') then duration else 0 end) as ae_emergency_duration
    , sum(case when activity_type = 'A&E' and activity_subtype in ('UCC', 'WiC', 'SDEC') then duration else 0 end) as ae_urgent_duration

    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Elective' then encounters else 0 end) as ip_el_encounters
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then encounters else 0 end) as ip_nel_emergency_encounters
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Maternity' then encounters else 0 end) as ip_nel_maternity_encounters
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Other' then encounters else 0 end) as ip_nel_other_encounters
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Elective' then cost else 0 end) as ip_el_cost
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then cost else 0 end) as ip_nel_emergency_cost
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Maternity' then cost else 0 end) as ip_nel_maternity_cost
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Other' then cost else 0 end) as ip_nel_other_cost
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Elective' then duration else 0 end) as ip_el_duration
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then duration else 0 end) as ip_nel_emergency_duration
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Maternity' then duration else 0 end) as ip_nel_maternity_duration
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Other' then duration else 0 end) as ip_nel_other_duration

    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFA-F2F', 'OPFA-NFTF') then encounters else 0 end) as op_fa_encounters
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFUP-F2F', 'OPFUP-NFTF') then encounters else 0 end) as op_fup_encounters
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPPROC') then encounters else 0 end) as op_proc_encounters
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFA-F2F', 'OPFA-NFTF') then cost else 0 end) as op_fa_cost
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFUP-F2F', 'OPFUP-NFTF') then cost else 0 end) as op_fup_cost
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPPROC') then cost else 0 end) as op_proc_cost
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFA-F2F', 'OPFA-NFTF') then duration else 0 end) as op_fa_duration
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFUP-F2F', 'OPFUP-NFTF') then duration else 0 end) as op_fup_duration
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPPROC') then duration else 0 end) as op_proc_duration

from 
    combined
group by 
    sk_patient_id, activity_month