-- Raw layer model for sus_apc.spell.episodes.critical_care_submitted.daily_activities
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures
-- This is a 1:1 passthrough from source with standardized column names
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
