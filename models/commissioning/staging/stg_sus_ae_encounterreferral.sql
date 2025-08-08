-- Staging model for sus_ae.EncounterReferral
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "Sequence_Number" as sequence_number,
    "Code" as code,
    "Activity_Service_Request_Date" as activity_service_request_date,
    "Activity_Service_Request_Time" as activity_service_request_time,
    "Assessment_Date" as assessment_date,
    "Assessment_Time" as assessment_time,
    "Is_Approved" as is_approved
from {{ source('sus_ae', 'EncounterReferral') }}
