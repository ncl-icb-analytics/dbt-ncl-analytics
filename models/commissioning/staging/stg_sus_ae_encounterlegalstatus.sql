-- Staging model for sus_ae.EncounterLegalStatus
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "Sequence_Number" as sequence_number,
    "SK_MentalHealthLegalStatus_ID" as sk_mentalhealthlegalstatus_id,
    "Assignment_Period_Start_Date" as assignment_period_start_date,
    "Assignment_Period_Start_Time" as assignment_period_start_time,
    "Expiry_Date" as expiry_date,
    "Expiry_Time" as expiry_time,
    "Is_Approved" as is_approved
from {{ source('sus_ae', 'EncounterLegalStatus') }}
