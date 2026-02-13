with 
base_encounters as (
    select *
    from {{ ref('int_sus_ae_encounters') }}
    where start_date between dateadd(month, -12, current_date()) and current_date()
    and sk_patient_id is not null and sk_patient_id != '1'
),  
emergency_admissions as (
    select sk_patient_id 
        , count(distinct visit_occurrence_id) as ae_respiratory_admission_12mo
    from {{ ref('int_sus_ip_encounters') }}
    where start_date between dateadd(month, -12, current_date()) and current_date()
    and sk_patient_id is not null and sk_patient_id != '1'
    and left(spell_admission_method, 1) = '2' -- Non-elective - emergency
    and visit_occurrence_id in (select visit_occurrence_id from {{ref('int_comm_chronic_lower_respiratory')}})
    group by 
        sk_patient_id
),
ae_encounter_summary as(
    select
        be.sk_patient_id
        , count(distinct case when is_injury_related = FALSE  -- Attended - not injury -- TO DO: stratify by department type
                then be.visit_occurrence_id end) as ae_ill_12mo
        , count(distinct case when is_injury_related = FALSE -- Attended - not injury
                and start_date between dateadd(month, -3, current_date()) and current_date() 
                then be.visit_occurrence_id end) as ae_ill_3mo
        , count(distinct case when is_injury_related = FALSE  -- Attended - not injury
                and start_date between dateadd(month, -1, current_date()) and current_date() 
                then be.visit_occurrence_id end) as ae_ill_1mo
        , count(distinct be.visit_occurrence_id) as ae_tot_12mo -- all attendances
        , count(distinct case when is_injury_related = TRUE-- all injuries
                then be.visit_occurrence_id end) as ae_inj_12mo
        , count(distinct case when clr.respiratory_encounter = true -- respiratory
                then be.visit_occurrence_id end) as ae_respiratory_attendance_12mo
        , count(distinct case when pod = 'AE-T1' -- Type 1 A&E
                then be.visit_occurrence_id end) as ae_t1_12mo
    from base_encounters be
    left join {{ref('int_comm_chronic_lower_respiratory')}} clr 
    on be.visit_occurrence_id = clr.visit_occurrence_id
    group by 
        be.sk_patient_id
)

SELECT
    a.sk_patient_id
    , zeroifnull(a.ae_ill_12mo) as ae_ill_12mo
    , zeroifnull(a.ae_ill_3mo) as ae_ill_3mo
    , zeroifnull(a.ae_ill_1mo) as ae_ill_1mo
    , zeroifnull(a.ae_tot_12mo) as ae_tot_12mo
    , zeroifnull(a.ae_inj_12mo) as ae_inj_12mo
    , zeroifnull(a.ae_t1_12mo) as ae_t1_12mo
    , zeroifnull(a.ae_respiratory_attendance_12mo) as ae_respiratory_attendance_12mo -- 500k million attendance >= admission
    , zeroifnull(ea.ae_respiratory_admission_12mo) as ae_respiratory_admission_12mo --  15k admission > attendance
from ae_encounter_summary as a
left join emergency_admissions ea
    on a.sk_patient_id = ea.sk_patient_id
