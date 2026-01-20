{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.patient.social_and_personal_circumstances \ndbt: source(''sus_apc'', ''spell.episodes.patient.social_and_personal_circumstances'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  SOCIAL_AND_PERSONAL_CIRCUMSTANCES_ID -> social_and_personal_circumstances_id\n  code -> code\n  recorded_timestamp -> recorded_timestamp\n  is_data_absent -> is_data_absent\n  data_absent_reason -> data_absent_reason\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "SOCIAL_AND_PERSONAL_CIRCUMSTANCES_ID" as social_and_personal_circumstances_id,
    "code" as code,
    "recorded_timestamp" as recorded_timestamp,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.episodes.patient.social_and_personal_circumstances') }}
