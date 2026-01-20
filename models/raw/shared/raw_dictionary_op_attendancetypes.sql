{{
    config(
        description="Raw layer (Reference data for outpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.OP.AttendanceTypes \ndbt: source(''dictionary_op'', ''AttendanceTypes'') \nColumns:\n  SK_AttendanceType -> sk_attendance_type\n  BK_AttendanceTypeCode -> bk_attendance_type_code\n  AttendantType -> attendant_type\n  AttendantTypeDesc -> attendant_type_desc\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_AttendanceType" as sk_attendance_type,
    "BK_AttendanceTypeCode" as bk_attendance_type_code,
    "AttendantType" as attendant_type,
    "AttendantTypeDesc" as attendant_type_desc,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_op', 'AttendanceTypes') }}
