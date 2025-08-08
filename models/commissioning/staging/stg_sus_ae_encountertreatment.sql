-- Staging model for sus_ae.EncounterTreatment
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "TreatmentNumber" as treatmentnumber,
    "TreatmentCode" as treatmentcode,
    "ProcedureDate" as proceduredate,
    "ActivityPeriod" as activityperiod
from {{ source('sus_ae', 'EncounterTreatment') }}
