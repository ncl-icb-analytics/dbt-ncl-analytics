select
    person_id,
    pseudo_nhs_number as sk_patient_id
from {{ref('raw_mhsds_bridging')}}