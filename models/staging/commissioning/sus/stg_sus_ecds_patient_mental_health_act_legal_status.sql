-- Staging layer: Mental Health Act legal status classifications recorded against an ECDS attendance. Multiple rows per attendance, sequenced by mental_health_act_legal_status_id.
-- 1:1 with raw_sus_ecds_patient_mental_health_act_legal_status - column renaming only, no joins or filtering.

select
    rownumber_id,
    primarykey_id,
    mental_health_act_legal_status_id,
    classification,
    start_date,
    start_time,
    expiry_date,
    expiry_time,
    dmic_import_log_id,
    assignment_timestamp,
    expiry_timestamp
from {{ ref('raw_sus_ecds_patient_mental_health_act_legal_status') }}
