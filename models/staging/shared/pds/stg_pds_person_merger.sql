select
    row_id,
    pseudo_nhs_number as sk_patient_id,
    pseudo_superseded_nhs_number as sk_patient_id_superseded,
    to_date(person_merger_business_effective_from_date) as event_from_date,
    to_date(person_merger_business_effective_to_date) as event_to_date
from {{ ref('raw_pds_pds_person_merger') }}
