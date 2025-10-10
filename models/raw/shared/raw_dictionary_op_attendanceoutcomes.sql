-- Raw layer model for dictionary_op.AttendanceOutcomes
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_AttendanceOutcome" as sk_attendance_outcome,
    "BK_AttendanceOutcome" as bk_attendance_outcome,
    "AttendanceOutcome" as attendance_outcome,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_op', 'AttendanceOutcomes') }}
