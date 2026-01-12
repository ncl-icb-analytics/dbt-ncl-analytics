{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.critical_care_submitted.daily_activities.high_cost_drugs \ndbt: source(''sus_apc'', ''spell.episodes.critical_care_submitted.daily_activities.high_cost_drugs'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  CRITICAL_CARE_SUBMITTED_ID -> critical_care_submitted_id\n  DAILY_ACTIVITIES_ID -> daily_activities_id\n  HIGH_COST_DRUGS_ID -> high_cost_drugs_id\n  high_cost_drugs -> high_cost_drugs\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "CRITICAL_CARE_SUBMITTED_ID" as critical_care_submitted_id,
    "DAILY_ACTIVITIES_ID" as daily_activities_id,
    "HIGH_COST_DRUGS_ID" as high_cost_drugs_id,
    "high_cost_drugs" as high_cost_drugs,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.episodes.critical_care_submitted.daily_activities.high_cost_drugs') }}
