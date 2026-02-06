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
    select olids_id, patient_id,pcn_code
    from {{ ref('inclusion_cohort')}}
    where eligible = 1
),
hba1c_measurements as(
    select il.patient_id,
        il.pcn_code,
        clinical_effective_date,
        hba1c_value_ifcc_standardised as value,
        hba1c_category as category,
        'hba1c' as measurement_type
    from {{ ref('int_hba1c_all')}} hb
    inner join inclusion_list il on il.olids_id = hb.person_id
)

select * from hba1c_measurements
-- union with other measurements