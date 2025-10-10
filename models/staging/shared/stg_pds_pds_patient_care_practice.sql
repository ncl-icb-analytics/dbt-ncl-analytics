select
    row_id,
    pseudo_nhs_number as sk_patient_id,
    primary_care_provider,
    primary_care_provider_business_effective_from_date,
    primary_care_provider_business_effective_to_date,
    reason_for_removal,
    der_ccg_of_registration,
    der_current_ccg_of_registration,
    der_icb_of_registration,
    der_current_icb_of_registration
from {{ ref('raw_pds_pds_patient_care_practice') }}
