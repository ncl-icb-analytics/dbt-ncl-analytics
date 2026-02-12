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
        hb.clinical_effective_date,
        hb.id as measurement_id,
        hb.hba1c_ifcc as value,
        hb.hba1c_category as category,
        'hba1c' as measurement_type
    from {{ ref('int_hba1c_all')}} hb 
    inner join inclusion_list il on il.olids_id = hb.person_id
    where hb.clinical_effective_date between dateadd(year, -5, current_date()) and current_date()
    qualify 
        case when hb.is_ifcc then 1 when hb.is_dcct then 2 else 3 end = 
        min(case when hb.is_ifcc then 1 when hb.is_dcct then 2 else 3 end) 
            over (partition by il.patient_id, hb.clinical_effective_date)
        and row_number() over (
            partition by il.patient_id, hb.clinical_effective_date, hb.hba1c_ifcc
            order by measurement_id
        ) = 1
)

select * from hba1c_measurements
-- union with other measurements