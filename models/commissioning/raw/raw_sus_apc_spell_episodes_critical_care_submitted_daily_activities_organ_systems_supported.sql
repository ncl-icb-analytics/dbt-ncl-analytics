-- Raw layer model for sus_apc.spell.episodes.critical_care_submitted.daily_activities.organ_systems_supported
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "CRITICAL_CARE_SUBMITTED_ID" as critical_care_submitted_id,
    "DAILY_ACTIVITIES_ID" as daily_activities_id,
    "ORGAN_SYSTEMS_SUPPORTED_ID" as organ_systems_supported_id,
    "organ_systems_supported" as organ_systems_supported,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.episodes.critical_care_submitted.daily_activities.organ_systems_supported') }}
