-- Staging model for sus_apc.spell.episodes.clinical_coding.observations
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

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
