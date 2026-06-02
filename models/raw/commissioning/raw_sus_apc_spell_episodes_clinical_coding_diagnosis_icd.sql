{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.clinical_coding.diagnosis.icd \ndbt: source(''sus_apc'', ''spell.episodes.clinical_coding.diagnosis.icd'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  ICD_ID -> icd_id\n  code -> code\n  present_on_admission -> present_on_admission\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "ICD_ID" as icd_id,
    "code" as code,
    "present_on_admission" as present_on_admission,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.episodes.clinical_coding.diagnosis.icd') }}
