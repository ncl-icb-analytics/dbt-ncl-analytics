-- Raw layer model for sus_apc.spell.critical_care_consolidated.allocated_episodes
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures
-- This is a 1:1 passthrough from source with standardized column names
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
