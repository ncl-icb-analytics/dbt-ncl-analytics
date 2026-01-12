{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.clinical_coding.observations \ndbt: source(''sus_apc'', ''spell.episodes.clinical_coding.observations'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  OBSERVATIONS_ID -> observations_id\n  code -> code\n  value -> value\n  ucum_unit_of_measurement -> ucum_unit_of_measurement\n  timestamp -> timestamp\n  is_data_absent -> is_data_absent\n  data_absent_reason -> data_absent_reason\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "OBSERVATIONS_ID" as observations_id,
    "code" as code,
    "value" as value,
    "ucum_unit_of_measurement" as ucum_unit_of_measurement,
    "timestamp" as timestamp,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.episodes.clinical_coding.observations') }}
