{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.critical_care_consolidated.allocated_episodes.daily_activities.organ_systems_supported \ndbt: source(''sus_apc'', ''spell.critical_care_consolidated.allocated_episodes.daily_activities.organ_systems_supported'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  CRITICAL_CARE_CONSOLIDATED_ID -> critical_care_consolidated_id\n  ALLOCATED_EPISODES_ID -> allocated_episodes_id\n  DAILY_ACTIVITIES_ID -> daily_activities_id\n  ORGAN_SYSTEMS_SUPPORTED_ID -> organ_systems_supported_id\n  organ_systems_supported -> organ_systems_supported\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CRITICAL_CARE_CONSOLIDATED_ID" as critical_care_consolidated_id,
    "ALLOCATED_EPISODES_ID" as allocated_episodes_id,
    "DAILY_ACTIVITIES_ID" as daily_activities_id,
    "ORGAN_SYSTEMS_SUPPORTED_ID" as organ_systems_supported_id,
    "organ_systems_supported" as organ_systems_supported,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.critical_care_consolidated.allocated_episodes.daily_activities.organ_systems_supported') }}
