{{
    config(
        description="Raw layer (SUS Monthly admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterReferral \ndbt: source(''sus_apc_monthly'', ''EncounterReferral'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_EncounterID -> sk_encounter_id_1\n  Sequence_Number -> sequence_number\n  Sequence_Number -> sequence_number_1\n  Code -> code\n  Code -> code_1\n  Activity_Service_Request_Date -> activity_service_request_date\n  Activity_Service_Request_Date -> activity_service_request_date_1\n  Activity_Service_Request_Time -> activity_service_request_time\n  Activity_Service_Request_Time -> activity_service_request_time_1\n  Assessment_Date -> assessment_date\n  Assessment_Date -> assessment_date_1\n  Assessment_Time -> assessment_time\n  Assessment_Time -> assessment_time_1\n  Is_Approved -> is_approved\n  Is_Approved -> is_approved_1"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_EncounterID" as sk_encounter_id_1,
    "Sequence_Number" as sequence_number,
    "Sequence_Number" as sequence_number_1,
    "Code" as code,
    "Code" as code_1,
    "Activity_Service_Request_Date" as activity_service_request_date,
    "Activity_Service_Request_Date" as activity_service_request_date_1,
    "Activity_Service_Request_Time" as activity_service_request_time,
    "Activity_Service_Request_Time" as activity_service_request_time_1,
    "Assessment_Date" as assessment_date,
    "Assessment_Date" as assessment_date_1,
    "Assessment_Time" as assessment_time,
    "Assessment_Time" as assessment_time_1,
    "Is_Approved" as is_approved,
    "Is_Approved" as is_approved_1
from {{ source('sus_apc_monthly', 'EncounterReferral') }}
