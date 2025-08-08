-- Staging model for sus_ae.EncounterInvestigation
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "InvestigationNumber" as investigationnumber,
    "InvestigationCode" as investigationcode,
    "ActivityPeriod" as activityperiod
from {{ source('sus_ae', 'EncounterInvestigation') }}
