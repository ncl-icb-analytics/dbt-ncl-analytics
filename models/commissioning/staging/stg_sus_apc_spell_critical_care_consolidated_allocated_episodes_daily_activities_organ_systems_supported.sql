-- Staging model for sus_apc.spell.critical_care_consolidated.allocated_episodes.daily_activities.organ_systems_supported
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CRITICAL_CARE_CONSOLIDATED_ID" as critical_care_consolidated_id,
    "ALLOCATED_EPISODES_ID" as allocated_episodes_id,
    "DAILY_ACTIVITIES_ID" as daily_activities_id,
    "ORGAN_SYSTEMS_SUPPORTED_ID" as organ_systems_supported_id,
    "organ_systems_supported" as organ_systems_supported,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_apc', 'spell.critical_care_consolidated.allocated_episodes.daily_activities.organ_systems_supported') }}
