-- Staging model for sus_ae.Encounter
-- Source: "DATA_LAKE"."SUS_AE"
-- Description: SUS emergency care attendances and activity

select
    "SK_EncounterID" as sk_encounterid,
    "RowID" as rowid,
    "SK_SUSDataMartID" as sk_susdatamartid,
    "ActivityPeriod" as activityperiod
from {{ source('sus_ae', 'Encounter') }}
