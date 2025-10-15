
with 
base_encounters as (
    select
        pp.sk_patient_id
        , encounter_id
        , start_date
        , actual_duration
        , national_slot_category_name
        , service_setting
        , code
        , display
    from {{ ref('int_gp_encounters_appt') }} gpa
    left join {{ref('dim_person_pseudo')}} pp on pp.person_id = gpa.person_id
    where start_date between dateadd(month, -12, current_date()) and current_date()
), 
gp_encounter_summary as(
    select
        sk_patient_id
        , count(distinct case when code not in ('3', '0') -- Attended (assumed - consider selecting for)
                then encounter_id end) as gp_att_tot_12mo
        , count(distinct case when code not in ('3', '0') -- Attended
                and start_date between dateadd(month, -3, current_date()) and current_date() 
                then encounter_id end) as gp_att_tot_3mo
        , count(distinct case when code not in ('3', '0') -- Attended
                and start_date between dateadd(month, -1, current_date()) and current_date() 
                then encounter_id end) as gp_att_tot_1mo
        , count(distinct encounter_id) as gp_app_tot_12mo
        , count(distinct case when code in ('3') -- Attended (assumed - consider selecting for)
                then encounter_id end) as gp_dna_tot_12mo
    from base_encounters
    group by 
        sk_patient_id
)

SELECT
    sk_patient_id
    , zeroifnull(gp_att_tot_12mo) as gp_att_tot_12mo
    , zeroifnull(gp_att_tot_3mo) as gp_att_tot_3mo
    , zeroifnull(gp_att_tot_1mo) as gp_att_tot_1mo
    , zeroifnull(gp_app_tot_12mo) as gp_app_tot_12mo
    , zeroifnull(gp_dna_tot_12mo) as gp_dna_tot_12mo
from 
    gp_encounter_summary as a
