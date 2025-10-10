select
    row_id,
    pseudo_nhs_number as sk_patient_id,
    year_month_of_birth,
    gender,
    date_of_death,
    death_status,
    preferred_language,
    interpreter_required,
    person_business_effective_from_date,
    person_business_effective_to_date
from {{ ref('raw_pds_pds_person') }}
