-- Staging model for sus_ae.patient.social_and_personal_circumstances
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "SOCIAL_AND_PERSONAL_CIRCUMSTANCES_ID" as social_and_personal_circumstances_id,
    "code" as code,
    "recorded_timestamp" as recorded_timestamp,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ae', 'patient.social_and_personal_circumstances') }}
