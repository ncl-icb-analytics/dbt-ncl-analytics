{{
    config(
        materialized='table')
}}


/*
Patient data processing for CLTCS

Clinical Purpose:
- Details LTCS summary data for patients with complex needs in C-LTCS

*/

with inclusion_list as (
    select patient_id, olids_id, pcn_code, pcn_name, practice_code, practice_name, age, main_language
    from {{ ref('inclusion_cohort')}}
    where eligible = 1
)

select il.patient_id
    , il.pcn_code
    , ltcs.condition_code
    , ltcs.condition_name
    , ltcs.clinical_domain
    , ltcs.is_on_register
    , ltcs.is_qof
    , ltcs.earliest_diagnosis_date
    , ltcs.latest_diagnosis_date
from inclusion_list il
inner join {{ ref('fct_person_ltc_summary')}} ltcs on il.olids_id = ltcs.person_id
