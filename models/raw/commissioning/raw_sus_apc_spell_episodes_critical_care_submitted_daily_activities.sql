{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.critical_care_submitted.daily_activities \ndbt: source(''sus_apc'', ''spell.episodes.critical_care_submitted.daily_activities'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  CRITICAL_CARE_SUBMITTED_ID -> critical_care_submitted_id\n  DAILY_ACTIVITIES_ID -> daily_activities_id\n  date -> date\n  weight -> weight\n  activity_to_episode_relationship -> activity_to_episode_relationship\n  validation.pbr_cc_indicator -> validation_pbr_cc_indicator\n  validation.cc_excluded_reason -> validation_cc_excluded_reason\n  dmicImportLogId -> dmic_import_log_id\n  critical_care_level -> critical_care_level"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "CRITICAL_CARE_SUBMITTED_ID" as critical_care_submitted_id,
    "DAILY_ACTIVITIES_ID" as daily_activities_id,
    "date" as date,
    "weight" as weight,
    "activity_to_episode_relationship" as activity_to_episode_relationship,
    "validation.pbr_cc_indicator" as validation_pbr_cc_indicator,
    "validation.cc_excluded_reason" as validation_cc_excluded_reason,
    "dmicImportLogId" as dmic_import_log_id,
    "critical_care_level" as critical_care_level
from {{ source('sus_apc', 'spell.episodes.critical_care_submitted.daily_activities') }}
