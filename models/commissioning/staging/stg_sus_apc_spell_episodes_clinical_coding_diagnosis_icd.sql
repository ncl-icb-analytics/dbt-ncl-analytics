-- Staging model for sus_apc.spell.episodes.clinical_coding.diagnosis.icd
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "present_on_admission" as present_on_admission,
    "dmicImportLogId" as dmicimportlogid,
    "ICD_ID" as icd_id,
    "code" as code,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "ROWNUMBER_ID" as rownumber_id
from {{ source('sus_apc', 'spell.episodes.clinical_coding.diagnosis.icd') }}
