with 
base_encounters as (
    select *
    from {{ ref('int_sus_apc_encounter') }}
    where start_date between dateadd(month, -12, current_date()) and current_date()
),

base_population as ( -- all GP registered patients
    select distinct sk_patient_id
    from {{ref('dim_person_demographics_basic')}}
    where sk_patient_id is not null and sk_patient_id != '1'
),  
apc_encounter_summary as(
    select
        sk_patient_id
        , count(distinct case when start_date between dateadd(month, -3, current_date()) and current_date() 
                then visit_occurrence_id end) as apc_3mo
        , count(distinct case when start_date between dateadd(month, -1, current_date()) and current_date() 
                then visit_occurrence_id end) as apc_1mo
        , count(distinct visit_occurrence_id) as apc_12mo 
        , sum(duration) as apc_los_12mo -- TO DO: add inferred los to open spells in int_table
        , count(distinct case when left(spell_admission_method, 1) = '2' -- Non-elective - emergency
                then visit_occurrence_id end) as apc_nel_12mo
    from base_encounters
    group by 
        sk_patient_id
),
ambulatory_sensitive_nel_encounters as ( -- ambulatory sensitive NEL admissions
    select sk_patient_id 
        , count(distinct visit_occurrence_id) as acs_nel_12mo
    from {{ref('int_comm_ambulatory_sensitive_nel') }} 
    where start_date between dateadd(month, -12, current_date()) and current_date()
    and sk_patient_id is not null and sk_patient_id != '1'
    group by sk_patient_id
),
population_spine as (
    select sk_patient_id from base_population
    union 
    select sk_patient_id from base_encounters
    union 
    select sk_patient_id from ambulatory_sensitive_nel_encounters
)

SELECT
    ps.sk_patient_id
    , zeroifnull(a.apc_3mo) as apc_3mo
    , zeroifnull(a.apc_1mo) as apc_1mo
    , zeroifnull(a.apc_12mo) as apc_12mo
    , zeroifnull(a.apc_los_12mo) as apc_los_12mo
    , zeroifnull(a.apc_nel_12mo) as apc_nel_12mo
    , zeroifnull(asc.acs_nel_12mo) as acs_nel_12mo
from population_spine ps
left join apc_encounter_summary as a
    on ps.sk_patient_id = a.sk_patient_id
left join ambulatory_sensitive_nel_encounters asc
    on ps.sk_patient_id = asc.sk_patient_id
where ps.sk_patient_id is not null and ps.sk_patient_id != '1'