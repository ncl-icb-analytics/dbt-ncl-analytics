-- Staging model for sus_apc.spell.episodes.critical_care_submitted.daily_activities.codes
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "CRITICAL_CARE_SUBMITTED_ID" as critical_care_submitted_id,
    "DAILY_ACTIVITIES_ID" as daily_activities_id,
    "CODES_ID" as codes_id,
    "codes" as codes,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_apc', 'spell.episodes.critical_care_submitted.daily_activities.codes') }}
