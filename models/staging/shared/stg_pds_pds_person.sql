select 
    row_id, 
    pseudo_nhs_number as sk_patient_id, 
    to_date(year_month_of_birth, 'YYYYMM') as year_month_of_birth, 
    gender as gender_code, 
    date_of_death, 
    death_status, 
    preferred_language as preferred_language_code, 
    interpreter_required as interpreter_required_code, 
    person_business_effective_from_date as event_from_date, 
    person_business_effective_to_date as event_to_date

from {{ref('raw_pds_pds_person')}}
