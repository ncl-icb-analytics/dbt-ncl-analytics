-- Staging model for sus_apc.spell.critical_care_consolidated.allocated_episodes.daily_activities
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CRITICAL_CARE_CONSOLIDATED_ID" as critical_care_consolidated_id,
    "ALLOCATED_EPISODES_ID" as allocated_episodes_id,
    "DAILY_ACTIVITIES_ID" as daily_activities_id,
    "date" as date,
    "weight" as weight,
    "unbundled_hrg_child_cc" as unbundled_hrg_child_cc,
    "tariff_days" as tariff_days,
    "length_of_stay" as length_of_stay,
    "dmicImportLogId" as dmic_import_log_id,
    "critical_care_level" as critical_care_level
from {{ source('sus_apc', 'spell.critical_care_consolidated.allocated_episodes.daily_activities') }}
