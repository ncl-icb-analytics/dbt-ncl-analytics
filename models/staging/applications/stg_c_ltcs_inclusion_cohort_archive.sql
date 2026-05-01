select cast(patient_id as varchar) as patient_id
    , area_code
    , practice_code
    , cohort_event
    , is_active
    , event_written_at
from {{ ref('raw_c_ltcs_cltcs_inclusion_cohort_archive') }}
