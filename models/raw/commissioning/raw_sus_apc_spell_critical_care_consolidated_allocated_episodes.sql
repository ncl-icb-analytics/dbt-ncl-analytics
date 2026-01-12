{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.critical_care_consolidated.allocated_episodes \ndbt: source(''sus_apc'', ''spell.critical_care_consolidated.allocated_episodes'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  CRITICAL_CARE_CONSOLIDATED_ID -> critical_care_consolidated_id\n  ALLOCATED_EPISODES_ID -> allocated_episodes_id\n  episode_identifier -> episode_identifier\n  unbundled_hrg_adult_cc -> unbundled_hrg_adult_cc\n  tariff_days -> tariff_days\n  length_of_stay -> length_of_stay\n  dmicImportLogId -> dmic_import_log_id\n  cc_period_aggregate_tariff -> cc_period_aggregate_tariff"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CRITICAL_CARE_CONSOLIDATED_ID" as critical_care_consolidated_id,
    "ALLOCATED_EPISODES_ID" as allocated_episodes_id,
    "episode_identifier" as episode_identifier,
    "unbundled_hrg_adult_cc" as unbundled_hrg_adult_cc,
    "tariff_days" as tariff_days,
    "length_of_stay" as length_of_stay,
    "dmicImportLogId" as dmic_import_log_id,
    "cc_period_aggregate_tariff" as cc_period_aggregate_tariff
from {{ source('sus_apc', 'spell.critical_care_consolidated.allocated_episodes') }}
