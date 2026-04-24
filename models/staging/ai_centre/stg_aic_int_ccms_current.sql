select
    person_id,
    cambridge_comorbidity_score,
    ccms_current_id,
    last_updated
from {{ ref('raw_aic_int_ccms_current') }}