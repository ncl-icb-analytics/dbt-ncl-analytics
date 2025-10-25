select
    row_id,
    pseudo_nhs_number as sk_patient_id,
    reason_for_removal,
    reason_for_removal_business_effective_from_date,
    reason_for_removal_business_effective_to_date
from {{ ref('raw_pds_pds_reason_for_removal') }}
