select
    person_id,
    nhs_number_pseudo as sk_patient_id,
    person_index_id,
    pseudo_nhs_number
from {{ref('raw_csds_bridging')}}