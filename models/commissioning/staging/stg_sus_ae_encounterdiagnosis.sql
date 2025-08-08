-- Staging model for sus_ae.EncounterDiagnosis
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "DiagnosisNumber" as diagnosisnumber,
    "DiagnosisCode" as diagnosiscode,
    "ActivityPeriod" as activityperiod
from {{ source('sus_ae', 'EncounterDiagnosis') }}
