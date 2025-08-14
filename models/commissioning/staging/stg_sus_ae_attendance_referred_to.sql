-- Staging model for sus_ae.attendance.referred_to
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "REFERRED_TO_ID" as referred_to_id,
    "service" as service,
    "is_code_approved" as is_code_approved,
    "request_date" as request_date,
    "request_time" as request_time,
    "assessment_date" as assessment_date,
    "assessment_time" as assessment_time,
    "dmicImportLogId" as dmicimportlogid,
    "request_timestamp" as request_timestamp,
    "assessment_timestamp" as assessment_timestamp
from {{ source('sus_ae', 'attendance.referred_to') }}
