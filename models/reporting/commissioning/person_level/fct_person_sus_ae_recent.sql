with 
base_encounters as (
    select *
    from {{ ref('stg_sus_ae_emergency_care') }}
    where attendance_arrival_date between dateadd(month, -12, current_date()) and current_date()
),  
ae_encounter_summary as(
    select
        sk_patient_id
        , count(distinct case when clinical_chief_complaint_is_injury_related = FALSE  -- Attended - not injury -- TO DO: stratify by department type
                then primarykey_id end) as ae_ill_12mo
        , count(distinct case when clinical_chief_complaint_is_injury_related = FALSE -- Attended - not injury
                and attendance_arrival_date between dateadd(month, -3, current_date()) and current_date() 
                then primarykey_id end) as ae_ill_3mo
        , count(distinct case when clinical_chief_complaint_is_injury_related = FALSE  -- Attended - not injury
                and attendance_arrival_date between dateadd(month, -1, current_date()) and current_date() 
                then primarykey_id end) as ae_ill_1mo
        , count(distinct primarykey_id) as ae_tot_12mo -- all attendances
        , count(distinct case when clinical_chief_complaint_is_injury_related = TRUE-- all injuries
                then primarykey_id end) as ae_inj_12mo
        , count(distinct case when attendance_location_department_type = '01'  -- Type 1 A&E
                then primarykey_id end) as ae_t1_12mo
    from base_encounters
    group by 
        sk_patient_id
)

SELECT
    sk_patient_id
    , zeroifnull(ae_ill_12mo) as ae_ill_12mo
    , zeroifnull(ae_ill_3mo) as ae_ill_3mo
    , zeroifnull(ae_ill_1mo) as ae_ill_1mo
    , zeroifnull(ae_tot_12mo) as ae_tot_12mo
    , zeroifnull(ae_inj_12mo) as ae_inj_12mo
    , zeroifnull(ae_t1_12mo) as ae_t1_12mo
from 
    ae_encounter_summary as a
where sk_patient_id is not null