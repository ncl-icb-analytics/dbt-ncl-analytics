-- Staging model for sus_ae.EncounterTreatmentSNOMED
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "Sequence_Number" as sequence_number,
    "Code" as code,
    "Date" as date,
    "Time" as time,
    "Is_Approved" as is_approved
from {{ source('sus_ae', 'EncounterTreatmentSNOMED') }}
