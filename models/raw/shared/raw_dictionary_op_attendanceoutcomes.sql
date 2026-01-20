{{
    config(
        description="Raw layer (Reference data for outpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.OP.AttendanceOutcomes \ndbt: source(''dictionary_op'', ''AttendanceOutcomes'') \nColumns:\n  SK_AttendanceOutcome -> sk_attendance_outcome\n  BK_AttendanceOutcome -> bk_attendance_outcome\n  AttendanceOutcome -> attendance_outcome\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_AttendanceOutcome" as sk_attendance_outcome,
    "BK_AttendanceOutcome" as bk_attendance_outcome,
    "AttendanceOutcome" as attendance_outcome,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_op', 'AttendanceOutcomes') }}
