{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.critical_care_consolidated.allocated_episodes.daily_activities \ndbt: source(''sus_apc'', ''spell.critical_care_consolidated.allocated_episodes.daily_activities'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  CRITICAL_CARE_CONSOLIDATED_ID -> critical_care_consolidated_id\n  ALLOCATED_EPISODES_ID -> allocated_episodes_id\n  DAILY_ACTIVITIES_ID -> daily_activities_id\n  date -> date\n  weight -> weight\n  unbundled_hrg_child_cc -> unbundled_hrg_child_cc\n  tariff_days -> tariff_days\n  length_of_stay -> length_of_stay\n  dmicImportLogId -> dmic_import_log_id\n  critical_care_level -> critical_care_level\n  unbundled_tariff -> unbundled_tariff"
    )
}}
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
    "critical_care_level" as critical_care_level,
    "unbundled_tariff" as unbundled_tariff
from {{ source('sus_apc', 'spell.critical_care_consolidated.allocated_episodes.daily_activities') }}
