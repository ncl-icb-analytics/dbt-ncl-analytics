-- Staging model for sus_apc.spell.episodes.patient.social_and_personal_circumstances
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "SOCIAL_AND_PERSONAL_CIRCUMSTANCES_ID" as social_and_personal_circumstances_id,
    "code" as code,
    "recorded_timestamp" as recorded_timestamp,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_apc', 'spell.episodes.patient.social_and_personal_circumstances') }}
