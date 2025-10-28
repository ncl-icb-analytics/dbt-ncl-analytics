-- Raw layer model for aic.BASE_SUS__APC_SPELL_EPISODES_CLINICAL_CODING_DIAGNOSIS_ICD
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "PRESENT_ON_ADMISSION" as present_on_admission,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "ICD_ID" as icd_id,
    "CODE" as code,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "ROWNUMBER_ID" as rownumber_id
from {{ source('aic', 'BASE_SUS__APC_SPELL_EPISODES_CLINICAL_CODING_DIAGNOSIS_ICD') }}
