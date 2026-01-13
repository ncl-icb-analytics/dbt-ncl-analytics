with 
base_encounters as (
    select *
    from {{ ref('int_sus_ae_encounters') }}
    where start_date between dateadd(month, -12, current_date()) and current_date()
),  
ae_encounter_summary as(
    select
        sk_patient_id
        , count(distinct case when is_injury_related = FALSE  -- Attended - not injury -- TO DO: stratify by department type
                then visit_occurrence_id end) as ae_ill_12mo
        , count(distinct case when is_injury_related = FALSE -- Attended - not injury
                and start_date between dateadd(month, -3, current_date()) and current_date() 
                then visit_occurrence_id end) as ae_ill_3mo
        , count(distinct case when is_injury_related = FALSE  -- Attended - not injury
                and start_date between dateadd(month, -1, current_date()) and current_date() 
                then visit_occurrence_id end) as ae_ill_1mo
        , count(distinct visit_occurrence_id) as ae_tot_12mo -- all attendances
        , count(distinct case when is_injury_related = TRUE-- all injuries
                then visit_occurrence_id end) as ae_inj_12mo
        , count(distinct case when pod = 'AE-T1' -- Type 1 A&E
                then visit_occurrence_id end) as ae_t1_12mo
        , count(distinct case when 
        -- respiratory complaints
                        (chief_complaint_ecds_group1 like 'Airway / breathing' and 
                         chief_complaint_code not in ('13094009', -- aponea in newborns
                                                        '70407001', -- stridor
                                                        '262599003')) -- foreign body in resp tract
                        -- respiratory primary diagnosis
                                or
                        primary_diagnosis in (select distinct snomed_code from {{ref('stg_dictionary_ecds_diagnosis')}} where ecds_group2 like 'Respiratory')  
                then visit_occurrence_id end) as ae_respiratory_complaint_12mo
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
    , zeroifnull(ae_respiratory_complaint_12mo) as ae_respiratory_complaint_12mo
from 
    ae_encounter_summary as a
where sk_patient_id is not null and sk_patient_id != '1'