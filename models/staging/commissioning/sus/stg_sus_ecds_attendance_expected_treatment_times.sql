-- Staging layer: Expected treatment times recorded against an ECDS attendance. Multiple rows per attendance, sequenced by expected_treatment_times_id.
-- 1:1 with raw_sus_ecds_attendance_expected_treatment_times - column renaming only, no joins or filtering.

select
    rownumber_id,
    primarykey_id,
    expected_treatment_times_id,
    timestamp,
    allocated_timestamp,
    dmic_import_log_id
from {{ ref('raw_sus_ecds_attendance_expected_treatment_times') }}
