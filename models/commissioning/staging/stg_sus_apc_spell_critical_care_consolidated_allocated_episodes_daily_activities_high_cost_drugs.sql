-- Staging model for sus_apc.spell.critical_care_consolidated.allocated_episodes.daily_activities.high_cost_drugs
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CRITICAL_CARE_CONSOLIDATED_ID" as critical_care_consolidated_id,
    "ALLOCATED_EPISODES_ID" as allocated_episodes_id,
    "DAILY_ACTIVITIES_ID" as daily_activities_id,
    "HIGH_COST_DRUGS_ID" as high_cost_drugs_id,
    "high_cost_drugs" as high_cost_drugs,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.critical_care_consolidated.allocated_episodes.daily_activities.high_cost_drugs') }}
