-- Staging model for sus_ae.EncounterCareProfessional
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "Sequence_Number" as sequence_number,
    "Code" as code,
    "SK_Issuer_Code_ID" as sk_issuer_code_id,
    "Tier" as tier,
    "Discharge_Responsibility_Indicator" as discharge_responsibility_indicator,
    "Is_Approved" as is_approved
from {{ source('sus_ae', 'EncounterCareProfessional') }}
