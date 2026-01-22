--Script to get key information from pds data
select
    sk_patient_id as pds_sk_patient_id,
    gender_code as pds_gender_code,
    coalesce(record_person_end_date, current_date()) as pds_gender_event_date,
    year_month_of_birth as pds_date_of_birth,
    coalesce(record_person_end_date, current_date()) as pds_dob_event_date,
    date_of_death as pds_date_of_death,
    coalesce(record_person_end_date, current_date()) as pds_death_event_date,
    preferred_language_code as pds_preferred_language_code,
    coalesce(record_person_end_date, current_date()) as pds_preferred_language_event_date,
    interpreter_required as pds_interpreter_required,
    coalesce(record_person_end_date, current_date()) as pds_interpreter_event_date,
    lsoa_21 as pds_lsoa_21,
    coalesce(record_residence_end_date, current_date()) as pds_residence_event_date,
    practice_code as pds_practice_code,
    coalesce(record_registered_end_date, current_date()) as pds_registered_event_date
    
from {{ref('int_person_pds_latest_record')}}