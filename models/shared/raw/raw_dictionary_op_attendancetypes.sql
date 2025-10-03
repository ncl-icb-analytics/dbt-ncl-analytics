-- Raw layer model for dictionary_op.AttendanceTypes
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_AttendanceType" as sk_attendance_type,
    "BK_AttendanceTypeCode" as bk_attendance_type_code,
    "AttendantType" as attendant_type,
    "AttendantTypeDesc" as attendant_type_desc,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_op', 'AttendanceTypes') }}
