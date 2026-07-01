with 
base_encounters_raw as (
    select *
    from {{ ref('int_sus_apc_encounter') }}
    where (start_date between dateadd(month, -12, current_date()) and current_date()
        or end_date between dateadd(month, -12, current_date()) and current_date())
    and sk_patient_id is not null and sk_patient_id != '1'
    and spell_admission_method not in ('2C', '82', '31') -- birth of a baby
),

base_encounters_imputed as (
    select
        * exclude (start_date, end_date),
        coalesce(
            start_date,
            case
                when end_date is not null and duration is not null
                    then dateadd(day, -duration, end_date)
            end
        ) as start_date,
        coalesce(
            end_date,
            case
                when start_date is not null and duration is not null
                    then dateadd(day, duration, start_date)
                when start_date is not null and duration is null
                    then current_date()
            end
        ) as end_date
    from base_encounters_raw
),

base_encounters_with_duration as (
    select
        * exclude (duration),
        coalesce(
            duration,
            datediff(day, start_date, end_date)
        ) as duration
    from base_encounters_imputed
),

base_encounters as (
    select
        *,
        case
            when start_date >= dateadd(month, -12, current_date())
                then duration
            else greatest(0, datediff(day, dateadd(month, -12, current_date()), end_date))
        end as in_year_duration
    from base_encounters_with_duration
),

base_dedup as (
    select * from base_encounters
    qualify row_number() over (
        partition by
            sk_patient_id,
            start_date,
            start_time,
            organisation_id,
            iff(
                start_date = end_date
                and (
                    sk_patient_id is null
                    or start_time is null
                    or organisation_id is null
                ),
                visit_occurrence_id,
                null
            )
        order by end_date desc nulls last, end_time desc nulls last, visit_occurrence_id desc
    ) = 1
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
        , count(distinct case when start_date between dateadd(month, -12, current_date()) and current_date()
                then visit_occurrence_id end) as apc_12mo 
        , sum(in_year_duration) as apc_los_12mo
        , count(distinct case when left(spell_admission_method, 1) = '2' -- Non-elective - emergency
                and spell_admission_method != '2C'
                then visit_occurrence_id end) as apc_nel_12mo
    from base_dedup
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
    select sk_patient_id from base_dedup
    union 
    select sk_patient_id from ambulatory_sensitive_nel_encounters
)

SELECT
    ps.sk_patient_id
    , greatest(0, zeroifnull(a.apc_3mo)) as apc_3mo
    , greatest(0, zeroifnull(a.apc_1mo)) as apc_1mo
    , greatest(0, zeroifnull(a.apc_12mo)) as apc_12mo
    , greatest(0, zeroifnull(a.apc_los_12mo)) as apc_los_12mo
    , greatest(0, zeroifnull(a.apc_nel_12mo)) as apc_nel_12mo
    , greatest(0, zeroifnull(asc.acs_nel_12mo)) as acs_nel_12mo
from population_spine ps
left join apc_encounter_summary as a
    on ps.sk_patient_id = a.sk_patient_id
left join ambulatory_sensitive_nel_encounters asc
    on ps.sk_patient_id = asc.sk_patient_id
where ps.sk_patient_id is not null and ps.sk_patient_id != '1'