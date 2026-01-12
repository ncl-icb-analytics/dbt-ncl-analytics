{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.clinical_coding.coded_scored_assessments \ndbt: source(''sus_apc'', ''spell.episodes.clinical_coding.coded_scored_assessments'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  CODED_SCORED_ASSESSMENTS_ID -> coded_scored_assessments_id\n  tool_type -> tool_type\n  person_score -> person_score\n  validation_timestamp -> validation_timestamp\n  is_data_absent -> is_data_absent\n  data_absent_reason -> data_absent_reason\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "CODED_SCORED_ASSESSMENTS_ID" as coded_scored_assessments_id,
    "tool_type" as tool_type,
    "person_score" as person_score,
    "validation_timestamp" as validation_timestamp,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.episodes.clinical_coding.coded_scored_assessments') }}
