-- Staging model for dictionary_op.AttendanceOutcomes
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments

select
    "SK_AttendanceOutcome" as sk_attendanceoutcome,
    "BK_AttendanceOutcome" as bk_attendanceoutcome,
    "AttendanceOutcome" as attendanceoutcome,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_op', 'AttendanceOutcomes') }}
