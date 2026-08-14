-- Staging layer: Services an ECDS attendance was referred to. Multiple rows per attendance, sequenced by referred_to_id.
-- 1:1 with raw_sus_ecds_attendance_referred_to - column renaming only, no joins or filtering.

select
    rownumber_id,
    primarykey_id,
    referred_to_id,
    service, 
    is_code_approved,
    request_date,
    request_time,
    assessment_date,
    assessment_time,
    dmic_import_log_id,
    request_timestamp,
    assessment_timestamp
from {{ ref('raw_sus_ecds_attendance_referred_to') }}
