{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.critical_care_consolidated.allocated_episodes.daily_activities.codes \ndbt: source(''sus_apc'', ''spell.critical_care_consolidated.allocated_episodes.daily_activities.codes'') \nColumns:\n  DAILY_ACTIVITIES_ID -> daily_activities_id\n  CODES_ID -> codes_id\n  codes -> codes\n  dmicImportLogId -> dmic_import_log_id\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  CRITICAL_CARE_CONSOLIDATED_ID -> critical_care_consolidated_id\n  ALLOCATED_EPISODES_ID -> allocated_episodes_id"
    )
}}
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
