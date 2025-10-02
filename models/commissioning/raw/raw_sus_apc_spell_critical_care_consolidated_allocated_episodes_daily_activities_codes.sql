-- Raw layer model for sus_apc.spell.critical_care_consolidated.allocated_episodes.daily_activities.codes
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures
-- This is a 1:1 passthrough from source with standardized column names
select
    "DAILY_ACTIVITIES_ID" as daily_activities_id,
    "CODES_ID" as codes_id,
    "codes" as codes,
    "dmicImportLogId" as dmic_import_log_id,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CRITICAL_CARE_CONSOLIDATED_ID" as critical_care_consolidated_id,
    "ALLOCATED_EPISODES_ID" as allocated_episodes_id
from {{ source('sus_apc', 'spell.critical_care_consolidated.allocated_episodes.daily_activities.codes') }}
