-- Staging model for dictionary_op.AttendanceTypes
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments

select
    "SK_AttendanceType" as sk_attendancetype,
    "BK_AttendanceTypeCode" as bk_attendancetypecode,
    "AttendantType" as attendanttype,
    "AttendantTypeDesc" as attendanttypedesc,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_op', 'AttendanceTypes') }}
