select
    threshold_rule_id,
    programme_or_guideline,
    description,
    patient_group,
    threshold_type,
    systolic_threshold,
    diastolic_threshold,
    operator,
    notes
from {{ ref('raw_reference_bp_thresholds') }}
