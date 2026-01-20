{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.clinical_coding.diagnosis.icd \ndbt: source(''sus_apc'', ''spell.episodes.clinical_coding.diagnosis.icd'') \nColumns:\n  present_on_admission -> present_on_admission\n  dmicImportLogId -> dmic_import_log_id\n  ICD_ID -> icd_id\n  code -> code\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  ROWNUMBER_ID -> rownumber_id"
    )
}}
select
    "present_on_admission" as present_on_admission,
    "dmicImportLogId" as dmic_import_log_id,
    "ICD_ID" as icd_id,
    "code" as code,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "ROWNUMBER_ID" as rownumber_id
from {{ source('sus_apc', 'spell.episodes.clinical_coding.diagnosis.icd') }}
