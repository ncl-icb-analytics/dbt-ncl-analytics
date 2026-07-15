select
    sk_patient_id,
    preference_type,
    preference_status,
    effective_from,
    effective_to,
    is_latest,
    lds_record_id,
    lds_business_id,
    lds_is_deleted
from {{ ref('raw_olids_national_data_opt_out') }}
where coalesce(lds_is_deleted, false) = false
