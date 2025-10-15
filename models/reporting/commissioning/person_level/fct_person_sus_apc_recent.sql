with 
base_encounters as (
    select *
    from {{ ref('stg_sus_apc_spell') }}
    where spell_admission_date between dateadd(month, -12, current_date()) and current_date()
),  
apc_encounter_summary as(
    select
        sk_patient_id
        , count(distinct case when spell_admission_date between dateadd(month, -3, current_date()) and current_date() 
                then primarykey_id end) as apc_3mo
        , count(distinct case when spell_admission_date between dateadd(month, -1, current_date()) and current_date() 
                then primarykey_id end) as apc_1mo
        , count(distinct primarykey_id) as apc_12mo 
        , sum(spell_discharge_length_of_hospital_stay) as apc_los_12mo -- TO DO: add inferred los to open spells in int_table
    from base_encounters
    group by 
        sk_patient_id
)

SELECT
    sk_patient_id
    , zeroifnull(apc_3mo) as apc_3mo
    , zeroifnull(apc_1mo) as apc_1mo
    , zeroifnull(apc_12mo) as apc_12mo
    , zeroifnull(apc_los_12mo) as apc_los_12mo
from 
    apc_encounter_summary as a

where sk_patient_id is not null